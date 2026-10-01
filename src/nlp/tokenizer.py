from pyvi import ViTokenizer

def tokenize_vietnamese(text: str) -> str:
    """
    Tách từ tiếng Việt bằng thư viện pyvi.
    Ví dụ: 'Dữ liệu lớn' -> 'Dữ_liệu lớn'
    """
    if not text:
        return ""
    try:
        return ViTokenizer.tokenize(text)
    except Exception:
        return text
