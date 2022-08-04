#![allow(dead_code)]

mod log {
    static LOGGER: SimpleLogger = SimpleLogger;

    struct SimpleLogger;

    impl log::Log for SimpleLogger {
        fn enabled(&self, metadata: &log::Metadata) -> bool {
            metadata.level() <= log::Level::Info
        }

        fn log(&self, record: &log::Record) {
            if self.enabled(record.metadata()) {
                println!("{} - {}", record.level(), record.args());
            }
        }

        fn flush(&self) {}
    }

    fn demo_log() {
        log::trace!("Commencing yak shaving");

        log::info!("Razor located: 1");

        log::warn!("Unable to locate a razor:, retrying");
    }

    #[test]
    fn test_log() {
        log::set_logger(&LOGGER).map(|()| log::set_max_level(log::LevelFilter::Info));

        demo_log()
    }
}

mod trace {
    use std::{error::Error, io};

    use tracing::{debug, error, info, span, warn, Level};

    // the `#[tracing::instrument]` attribute creates and enters a span
    // every time the instrumented function is called. The span is named after
    // the function or method. Parameters passed to the function are recorded as fields.
    #[tracing::instrument]
    pub fn shave(yak: usize) -> Result<(), Box<dyn Error + 'static>> {
        // this creates an event at the DEBUG level with two fields:
        // - `excitement`, with the key "excitement" and the value "yay!"
        // - `message`, with the key "message" and the value "hello! I'm gonna shave a yak."
        //
        // unlike other fields, `message`'s shorthand initialization is just the string itself.
        debug!(excitement = "yay!", "hello! I'm gonna shave a yak.");
        if yak == 3 {
            warn!("could not locate yak!");
            // note that this is intended to demonstrate `tracing`'s features, not idiomatic
            // error handling! in a library or application, you should consider returning
            // a dedicated `YakError`. libraries like snafu or thiserror make this easy.
            return Err(io::Error::new(io::ErrorKind::Other, "shaving yak failed!").into());
        } else {
            debug!("yak shaved successfully");
        }
        Ok(())
    }

    pub fn shave_all(yaks: usize) -> usize {
        // Constructs a new span named "shaving_yaks" at the TRACE level,
        // and a field whose key is "yaks". This is equivalent to writing:
        //
        // let span = span!(Level::TRACE, "shaving_yaks", yaks = yaks);
        //
        // local variables (`yaks`) can be used as field values
        // without an assignment, similar to struct initializers.
        let span = span!(Level::TRACE, "shaving_yaks", yaks);
        let _enter = span.enter();

        info!("shaving yaks");

        let mut yaks_shaved = 0;
        for yak in 1..=yaks {
            let res = shave(yak);
            debug!(yak, shaved = res.is_ok());

            if let Err(ref error) = res {
                // Like spans, events can also use the field initialization shorthand.
                // In this instance, `yak` is the field being initialized.
                error!(yak, error = error.as_ref(), "failed to shave yak!");
            } else {
                yaks_shaved += 1;
            }
            debug!(yaks_shaved);
        }

        yaks_shaved
    }

    #[test]
    fn test_trace() {
        tracing_subscriber::fmt::init();

        shave_all(10);
    }
}

fn main() {}
