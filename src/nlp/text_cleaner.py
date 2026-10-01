import re
from pyspark.sql import functions as F

HTML_REGEX = r"<[^>]+>"
WHITESPACE_REGEX = r"\s+"

def clean_html_regex(text: str) -> str:
    """
    Làm sạch thẻ HTML và khoảng trắng thừa bằng regex thuần (Python).
    """
    if not text:
        return ""
    text = re.sub(HTML_REGEX, " ", text)
    text = re.sub(WHITESPACE_REGEX, " ", text)
    return text.strip()

def clean_text_spark(df, input_col: str, output_col: str):
    """
    Làm sạch cột văn bản trực tiếp trên Spark DataFrame bằng hàm tối ưu nội tại.
    """
    return df.withColumn(
        output_col,
        F.trim(F.regexp_replace(F.regexp_replace(F.col(input_col), HTML_REGEX, " "), WHITESPACE_REGEX, " "))
    )
