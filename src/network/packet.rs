/// Packet types for potential future custom protocol implementation.
/// Currently, the network layer uses `bincode` serialization with `DatabaseRequest`/`DatabaseResponse`.
#[allow(dead_code)]
#[derive(Debug)]
enum PacketType {
    Request,
    Response,
    Error,
}

/// Raw packet structure for potential future custom protocol implementation.
#[allow(dead_code)]
#[derive(Debug)]
struct Packet {
    packet_type: PacketType,
    data: Vec<u8>,
}
