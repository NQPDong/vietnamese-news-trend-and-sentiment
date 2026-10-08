"""
Danh sách từ dừng tiếng Việt chọn lọc phục vụ tiền xử lý NLP trong Spark MLlib.
Loại bỏ các hư từ ngữ pháp, giữ lại các từ mang sắc thái phủ định hoặc cảm xúc mạnh.
"""

VIETNAMESE_STOPWORDS = [
    "và", "của", "là", "có", "được", "cho", "trong", "đã", "với", "các",
    "những", "một", "về", "người", "khi", "ở", "này", "đến", "từ", "ra",
    "để", "sau", "theo", "lại", "nhiều", "vào", "lên", "như", "nhưng", "đó",
    "vì", "tại", "nếu", "bởi", "do", "qua", "trên", "dưới", "cùng", "hoặc",
    "thì", "mà", "sẽ", "đang", "phải", "nên", "vẫn", "cứ", "chỉ", "cũng"
]
