#[derive(Debug)]
enum PacketType {
    Request,
    Response,
    Error,
}

#[derive(Debug)]
struct Packet {
    packet_type: PacketType,
    data: Vec<u8>,
}
