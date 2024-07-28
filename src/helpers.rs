// Helper function to format binary data for debugging
pub fn format_slice(slice: &[u8]) -> String {
    slice.iter().map(|byte| format!("{:02x} ", byte)).collect::<String>()
}