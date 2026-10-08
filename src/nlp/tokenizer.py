try:
    from pyvi import ViTokenizer
except ImportError:
    ViTokenizer = None

def tokenize_vietnamese(text: str) -> str:
    """
    Tách từ tiếng Việt bằng thư viện pyvi.
    Ví dụ: 'Dữ liệu lớn' -> 'Dữ_liệu lớn'
    """
    if not text:
        return ""
    if ViTokenizer is None:
        return text
    try:
        return ViTokenizer.tokenize(text)
    except Exception:
        return text
