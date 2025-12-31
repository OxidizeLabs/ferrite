/// Implements the codec for the network protocol.
///
/// Note: Currently unused - the network layer uses `bincode` serialization directly.
/// This struct is reserved for future custom protocol encoding if needed.
#[allow(dead_code)]
pub struct NetworkCodec {}

impl NetworkCodec {
    /// Creates a new `NetworkCodec` instance.
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {}
    }
}
