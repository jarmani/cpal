//! Sndio backend implementation.
//!
//! Default backend on OpenBSD.

use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::traits::{DeviceTrait, HostTrait, StreamTrait};
use crate::{
    BufferSize, BuildStreamError, Data, DefaultStreamConfigError, DeviceDescription,
    DeviceDescriptionBuilder, DeviceId, DeviceIdError, DeviceNameError, DevicesError,
    OutputCallbackInfo, OutputStreamTimestamp, PauseStreamError, PlayStreamError, SampleFormat,
    StreamConfig, StreamError, SupportedBufferSize, SupportedStreamConfig, SupportedStreamConfigRange,
    SupportedStreamConfigsError, I24, U24
};

use dasp_sample::{Sample, FromSample};

pub struct Host;

/// Content is false if the iterator is empty.
pub struct Devices(bool);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Device;

pub struct Stream {
    inner: Arc<StreamInner>,
}

struct StreamInner {
    hdl: Mutex<sndio::Sndio>,
    sample_format: SampleFormat,
    channels: u16,
    sample_rate: u32,
    buffer_frames: usize,
    state: Mutex<StreamState>,
    state_ready: Condvar,
    thread: Mutex<Option<JoinHandle<()>>>,
}

struct StreamState {
    started: bool,
    playing: bool,
    shutdown: bool,
    start_instant: Option<Instant>,
}

// Sndio streams are safe to send and share between threads.
unsafe impl Send for Stream {}
unsafe impl Sync for Stream {}

// Compile-time assertion that Stream is Send and Sync
crate::assert_stream_send!(Stream);
crate::assert_stream_sync!(Stream);

pub use crate::iter::{SupportedInputConfigs, SupportedOutputConfigs};

impl Host {
    pub fn new() -> Result<Self, crate::HostUnavailable> {
        Ok(Host)
    }
}

impl Devices {
    fn new() -> Result<Self, DevicesError> {
        Ok(Devices(true))
    }
}

impl Device {
    fn description(&self) -> Result<DeviceDescription, DeviceNameError> {
        Ok(DeviceDescriptionBuilder::new("Default Device".to_string())
            .driver("sndio".to_string())
            .direction(crate::DeviceDirection::Output)
            .build())
    }

    fn id(&self) -> Result<DeviceId, DeviceIdError> {
        Ok(DeviceId(
            crate::platform::HostId::Sndio,
            "default".to_string(),
        ))
    }

    fn supported_input_configs(
        &self,
    ) -> Result<SupportedInputConfigs, SupportedStreamConfigsError> {
        Ok(Vec::new().into_iter())
    }

    fn supported_output_configs(
        &self,
    ) -> Result<SupportedOutputConfigs, SupportedStreamConfigsError> {
        let config = match default_device_par() {
            Ok(par) => {
                let channels = if par.pchan == 0 { 1 } else { par.pchan as u16 };
                let rate = if par.rate == 0 { 44_100 } else { par.rate };
                let sample_format = sample_format_from_par(&par)
                    .ok_or(SupportedStreamConfigsError::InvalidArgument)?;
                SupportedStreamConfigRange::new(
                    channels,
                    rate,
                    rate,
                    SupportedBufferSize::Unknown,
                    sample_format,
                )
            }
            Err(_) => fallback_output_config_range(),
        };
        Ok(vec![config].into_iter())
    }

    fn default_input_config(&self) -> Result<SupportedStreamConfig, DefaultStreamConfigError> {
        Err(DefaultStreamConfigError::StreamTypeNotSupported)
    }

    fn default_output_config(&self) -> Result<SupportedStreamConfig, DefaultStreamConfigError> {
        const EXPECT: &str = "expected at least one valid sndio stream config";
        let range = self
            .supported_output_configs()
            .map_err(|_| DefaultStreamConfigError::DeviceNotAvailable)?
            .max_by(|a, b| a.cmp_default_heuristics(b))
            .expect(EXPECT);
        let config = range.with_sample_rate(range.max_sample_rate());

        Ok(config)
    }
}

impl HostTrait for Host {
    type Devices = Devices;
    type Device = Device;

    fn is_available() -> bool {
        true
    }

    fn devices(&self) -> Result<Self::Devices, DevicesError> {
        Devices::new()
    }

    fn default_input_device(&self) -> Option<Self::Device> {
        None
    }

    fn default_output_device(&self) -> Option<Self::Device> {
        Some(Device)
    }
}

impl DeviceTrait for Device {
    type SupportedInputConfigs = SupportedInputConfigs;
    type SupportedOutputConfigs = SupportedOutputConfigs;
    type Stream = Stream;

    fn description(&self) -> Result<DeviceDescription, DeviceNameError> {
        Device::description(self)
    }

    fn id(&self) -> Result<DeviceId, DeviceIdError> {
        Device::id(self)
    }

    fn supported_input_configs(
        &self,
    ) -> Result<Self::SupportedInputConfigs, SupportedStreamConfigsError> {
        Device::supported_input_configs(self)
    }

    fn supported_output_configs(
        &self,
    ) -> Result<Self::SupportedOutputConfigs, SupportedStreamConfigsError> {
        Device::supported_output_configs(self)
    }

    fn default_input_config(&self) -> Result<SupportedStreamConfig, DefaultStreamConfigError> {
        Device::default_input_config(self)
    }

    fn default_output_config(&self) -> Result<SupportedStreamConfig, DefaultStreamConfigError> {
        Device::default_output_config(self)
    }

    fn build_input_stream_raw<D, E>(
        &self,
        _config: &StreamConfig,
        _sample_format: SampleFormat,
        _data_callback: D,
        _error_callback: E,
        _timeout: Option<Duration>,
    ) -> Result<Self::Stream, BuildStreamError>
    where
        D: FnMut(&Data, &crate::InputCallbackInfo) + Send + 'static,
        E: FnMut(StreamError) + Send + 'static,
    {
        Err(BuildStreamError::StreamConfigNotSupported)
    }

    fn build_output_stream_raw<D, E>(
        &self,
        config: &StreamConfig,
        sample_format: SampleFormat,
        data_callback: D,
        error_callback: E,
        _timeout: Option<Duration>,
    ) -> Result<Self::Stream, BuildStreamError>
    where
        D: FnMut(&mut Data, &OutputCallbackInfo) + Send + 'static,
        E: FnMut(StreamError) + Send + 'static,
    {
        let sample_info = sample_format_to_sndio(sample_format)
            .ok_or(BuildStreamError::StreamConfigNotSupported)?;

        let mut hdl = open_playback_handle()?;
        let (par, buffer_frames) = configure_handle(&mut hdl, config, sample_info)?;

        let inner = Arc::new(StreamInner {
            hdl: Mutex::new(hdl),
            sample_format,
            channels: par.pchan as u16,
            sample_rate: par.rate,
            buffer_frames,
            state: Mutex::new(StreamState {
                started: false,
                playing: false,
                shutdown: false,
                start_instant: None,
            }),
            state_ready: Condvar::new(),
            thread: Mutex::new(None),
        });

        let thread_inner = Arc::clone(&inner);
        let join_handle = thread::Builder::new()
            .name("cpal-sndio-output".to_string())
            .spawn(move || output_loop(thread_inner, data_callback, error_callback))
            .map_err(|_| BuildStreamError::DeviceNotAvailable)?;
        *inner.thread.lock().unwrap() = Some(join_handle);

        Ok(Stream { inner })
    }
}

impl StreamTrait for Stream {
    fn play(&self) -> Result<(), PlayStreamError> {
        self.inner.play()
    }

    fn pause(&self) -> Result<(), PauseStreamError> {
        self.inner.pause()
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.shutdown = true;
        state.playing = false;
        self.inner.state_ready.notify_all();
        drop(state);

        if let Some(handle) = self.inner.thread.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

impl StreamInner {
    fn play(&self) -> Result<(), PlayStreamError> {
        let mut state = self.state.lock().unwrap();
        if state.shutdown {
            return Err(PlayStreamError::DeviceNotAvailable);
        }
        if !state.started {
            let ok = self.hdl.lock().unwrap().start();
            if !ok {
                return Err(PlayStreamError::DeviceNotAvailable);
            }
            state.started = true;
            state.start_instant = Some(Instant::now());
        }
        state.playing = true;
        self.state_ready.notify_all();
        Ok(())
    }

    fn pause(&self) -> Result<(), PauseStreamError> {
        let mut state = self.state.lock().unwrap();
        if state.shutdown {
            return Err(PauseStreamError::DeviceNotAvailable);
        }
        if state.playing {
            state.playing = false;
            self.state_ready.notify_all();
            if state.started {
                let _ = self.hdl.lock().unwrap().stop();
                state.started = false;
            }
        }
        Ok(())
    }
}

impl Iterator for Devices {
    type Item = Device;

    fn next(&mut self) -> Option<Device> {
        if self.0 {
            self.0 = false;
            Some(Device)
        } else {
            None
        }
    }
}

fn output_loop<D, E>(inner: Arc<StreamInner>, mut data_callback: D, mut error_callback: E)
where
    D: FnMut(&mut Data, &OutputCallbackInfo) + Send + 'static,
    E: FnMut(StreamError) + Send + 'static,
{
    match inner.sample_format {
        SampleFormat::I8 => {
            output_loop_typed::<i8, _, _>(inner, &mut data_callback, &mut error_callback)
        }
        SampleFormat::U8 => {
            output_loop_typed::<u8, _, _>(inner, &mut data_callback, &mut error_callback)
        }
        SampleFormat::I16 => {
            output_loop_typed::<i16, _, _>(inner, &mut data_callback, &mut error_callback)
        }
        SampleFormat::U16 => {
            output_loop_typed::<u16, _, _>(inner, &mut data_callback, &mut error_callback)
        }
        SampleFormat::I24 => {
            output_loop_typed::<I24, _, _>(inner, &mut data_callback, &mut error_callback)
        }
        SampleFormat::U24 => {
            output_loop_typed::<U24, _, _>(inner, &mut data_callback, &mut error_callback)
        }
        SampleFormat::I32 => {
            output_loop_typed::<i32, _, _>(inner, &mut data_callback, &mut error_callback)
        }
        SampleFormat::U32 => {
            output_loop_typed::<u32, _, _>(inner, &mut data_callback, &mut error_callback)
        }
        SampleFormat::F32 => {
            output_loop_typed::<f32, _, _>(inner, &mut data_callback, &mut error_callback)
        }
        SampleFormat::I64 => {
            output_loop_typed::<i64, _, _>(inner, &mut data_callback, &mut error_callback)
        }
        SampleFormat::U64 => {
            output_loop_typed::<u64, _, _>(inner, &mut data_callback, &mut error_callback)
        }
        SampleFormat::F64 => {
            output_loop_typed::<f64, _, _>(inner, &mut data_callback, &mut error_callback)
        }
    }
}

fn output_loop_typed<T, D, E>(inner: Arc<StreamInner>, data_callback: &mut D, error_callback: &mut E)
where
    T: Copy + Default,
    D: FnMut(&mut Data, &OutputCallbackInfo),
    E: FnMut(StreamError),
    i32: FromSample<T>,
{
    let non_native = vec![
        SampleFormat::F32,
        SampleFormat::I64,
        SampleFormat::U64,
        SampleFormat::F64];
    let sample_count = inner.buffer_frames * inner.channels as usize;
    let mut callback_buffer = vec![T::default(); sample_count];
    let mut device_buffer = if non_native.contains(&inner.sample_format) {
        /* will convert u64 into i32 to keep it simple, shorter */
        Some(vec![i32::default(); sample_count])
    } else {
        None
    };

    loop {
        let start_instant = {
            let mut state = inner.state.lock().unwrap();
            while !state.playing && !state.shutdown {
                state = inner.state_ready.wait(state).unwrap();
            }
            if state.shutdown {
                break;
            }
            state.start_instant.get_or_insert_with(Instant::now);
            *state.start_instant.as_ref().unwrap()
        };

        let elapsed = start_instant.elapsed();
        let callback = crate::StreamInstant::new(
            elapsed.as_secs() as i64,
            elapsed.subsec_nanos(),
        );
        let buffer_duration =
            Duration::from_secs_f64(inner.buffer_frames as f64 / inner.sample_rate as f64);
        let playback = callback.add(buffer_duration).unwrap_or(callback);
        let timestamp = OutputStreamTimestamp { callback, playback };
        let info = OutputCallbackInfo::new(timestamp);

        let mut data = unsafe {
            Data::from_parts(callback_buffer.as_mut_ptr() as *mut (),
            callback_buffer.len(), inner.sample_format)
        };
        data_callback(&mut data, &info);

        let bytes = if let Some(ref mut dev_buf) = device_buffer {
            for (dst, src) in dev_buf.iter_mut().zip(callback_buffer.iter()) {
                *dst = i32::from_sample(*src);
            }
            unsafe {
                std::slice::from_raw_parts(
                    dev_buf.as_ptr() as *const u8,
                    dev_buf.len() * std::mem::size_of::<i32>(),
                )
            }
        } else {
            data.bytes()
        };
        if !write_all(&inner.hdl, bytes) {
            let mut state = inner.state.lock().unwrap();
            state.shutdown = true;
            state.playing = false;
            inner.state_ready.notify_all();
            error_callback(StreamError::DeviceNotAvailable);
            break;
        }
    }
}

fn write_all(hdl: &Mutex<sndio::Sndio>, mut data: &[u8]) -> bool {
    let mut hdl = hdl.lock().unwrap();
    while !data.is_empty() {
        let written = hdl.write(data);
        if written == 0 {
            return false;
        }
        data = &data[written as usize..];
    }
    true
}

fn open_playback_handle() -> Result<sndio::Sndio, BuildStreamError> {
    sndio::Sndio::open(None, sndio::Mode::PLAY, false)
        .ok_or(BuildStreamError::DeviceNotAvailable)
}

fn configure_handle(
    hdl: &mut sndio::Sndio,
    config: &StreamConfig,
    sample_info: SampleFormatInfo,
) -> Result<(sndio::Par, usize), BuildStreamError> {
    let mut par = sndio::Sndio::init_par();
    par.rate = config.sample_rate;
    par.pchan = config.channels as u32;
    par.rchan = 0;
    par.bits = sample_info.bits;
    par.bps = sample_info.bps;
    par.sig = sample_info.sig;
    par.le = sndio::SIO_LE_NATIVE as u32;
    par.msb = 1;
    if let BufferSize::Fixed(frames) = config.buffer_size {
        par.appbufsz = frames;
    }

    let ok = hdl.set_par(&mut par);
    if !ok {
        return Err(BuildStreamError::StreamConfigNotSupported);
    }
    let ok = hdl.get_par(&mut par);
    if !ok {
        return Err(BuildStreamError::DeviceNotAvailable);
    }

    if par.pchan != config.channels as u32 || par.rate != config.sample_rate {
        return Err(BuildStreamError::StreamConfigNotSupported);
    }
    if par.bits != sample_info.bits || par.sig != sample_info.sig {
        return Err(BuildStreamError::StreamConfigNotSupported);
    }
    if par.rate == 0 {
        return Err(BuildStreamError::StreamConfigNotSupported);
    }

    let buffer_frames = match config.buffer_size {
        BufferSize::Fixed(frames) => frames as usize,
        BufferSize::Default => {
            if par.round > 0 {
                par.round as usize
            } else if par.appbufsz > 0 {
                par.appbufsz as usize
            } else if par.bufsz > 0 {
                par.bufsz as usize
            } else {
                1024
            }
        }
    };

    Ok((par, buffer_frames))
}

fn default_device_par() -> Result<sndio::Par, BuildStreamError> {
    let mut hdl = open_playback_handle()?;
    let mut par = sndio::Sndio::init_par();
    par.rate = 44_100;
    par.pchan = 2;
    par.rchan = 0;
    par.bits = 16;
    par.bps = 2;
    par.sig = 1;
    par.le = sndio::SIO_LE_NATIVE as u32;
    par.msb = 1;
    let ok = hdl.set_par(&mut par);
    if !ok {
        return Err(BuildStreamError::StreamConfigNotSupported);
    }
    let ok = hdl.get_par(&mut par);
    if !ok {
        Err(BuildStreamError::DeviceNotAvailable)
    } else {
        Ok(par)
    }
}

struct SampleFormatInfo {
    bits: u32,
    bps: u32,
    sig: u32,
}

fn sample_format_to_sndio(sample_format: SampleFormat) -> Option<SampleFormatInfo> {
    match sample_format {
        SampleFormat::I8 => Some(SampleFormatInfo {
            bits: 8,
            bps: 1,
            sig: 1,
        }),
        SampleFormat::U8 => Some(SampleFormatInfo {
            bits: 8,
            bps: 1,
            sig: 0,
        }),
        SampleFormat::I16 => Some(SampleFormatInfo {
            bits: 16,
            bps: 2,
            sig: 1,
        }),
        SampleFormat::U16 => Some(SampleFormatInfo {
            bits: 16,
            bps: 2,
            sig: 0,
        }),
        SampleFormat::I24 => Some(SampleFormatInfo {
            bits: 24,
            bps: 4,
            sig: 1,
        }),
        SampleFormat::U24 => Some(SampleFormatInfo {
            bits: 24,
            bps: 4,
            sig: 0,
        }),
        SampleFormat::I32 => Some(SampleFormatInfo {
            bits: 32,
            bps: 4,
            sig: 1,
        }),
        SampleFormat::U32 => Some(SampleFormatInfo {
            bits: 32,
            bps: 4,
            sig: 0,
        }),
        /* non native: encode into i32, must match with output_loop_typed() */
        SampleFormat::F32 => Some(SampleFormatInfo {
            bits: 32,
            bps: 4,
            sig: 1,
        }),
        SampleFormat::I64 => Some(SampleFormatInfo {
            bits: 32,
            bps: 4,
            sig: 1,
        }),
        SampleFormat::U64 => Some(SampleFormatInfo {
            bits: 32,
            bps: 4,
            sig: 1,
        }),
        SampleFormat::F64 => Some(SampleFormatInfo {
            bits: 32,
            bps: 4,
            sig: 1,
        }),
    }
}

/* 
 * There isn't *64 nor F32 because sndio par doesn't support such format.
 * While the backend can support those formats by re-encoding samples,
 * we deliberately do not advertise theim. Thus, consumers checking
 * supported_output_configs() will use the most efficient SampleFormat.
 */
fn sample_format_from_par(par: &sndio::Par) -> Option<SampleFormat> {
    match (par.bits, par.sig) {
        (8, 1) => Some(SampleFormat::I8),
        (8, 0) => Some(SampleFormat::U8),
        (16, 1) => Some(SampleFormat::I16),
        (16, 0) => Some(SampleFormat::U16),
        (24, 1) => Some(SampleFormat::I24),
        (24, 0) => Some(SampleFormat::U24),
        (32, 1) => Some(SampleFormat::I32),
        (32, 0) => Some(SampleFormat::U32),
        _ => None,
    }
}

fn fallback_output_config_range() -> SupportedStreamConfigRange {
    SupportedStreamConfigRange::new(
        2,
        44_100,
        44_100,
        SupportedBufferSize::Unknown,
        SampleFormat::I16,
    )
}
