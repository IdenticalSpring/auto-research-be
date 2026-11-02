/**
 * Sinh ObjectId 24-hex hợp lệ từ seed bất kỳ.
 *
 * `write_content`, `propose_method`, `inner_chatbot`… yêu cầu `document_id` là
 * ObjectId 24 ký tự hex. Tách riêng module để `mongo-fixtures` và `live-cases`
 * dùng chung mà không tạo vòng import.
 */
export function objectId(seed: string): string {
  const hex = seed
    .toLowerCase()
    .replace(/[^0-9a-f]/g, '')
    .slice(-24);
  return hex.padStart(24, '0');
}
