/// Formats a byte slice as a hexadecimal string for debugging.
///
/// Each byte is represented as a two-digit lowercase hex value followed
/// by a space (e.g., `"0a 1b 2c "`).
///
/// # Parameters
/// - `slice`: The byte slice to format.
///
/// # Returns
/// A `String` containing the hex representation of each byte.
///
/// # Example
/// ```rust,ignore
/// let data = [0x0A, 0x1B, 0x2C];
/// assert_eq!(format_slice(&data), "0a 1b 2c ");
/// ```
pub fn format_slice(slice: &[u8]) -> String {
    slice
        .iter()
        .map(|byte| format!("{:02x} ", byte))
        .collect::<String>()
}
