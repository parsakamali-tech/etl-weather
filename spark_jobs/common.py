import yaml
from pyspark.sql import SparkSession


# تعریف تابعی به نام load_config که یک پارامتر اختیاری به نام path می‌گیرد
# مقدار پیش‌فرض path برابر با "config/config.yaml" است
def load_config(path="config/config.yaml"):
    
    # باز کردن فایل با مسیر مشخص شده در حالت خواندن ("r")
    # استفاده از with برای اطمینان از بسته شدن خودکار فایل پس از اتمام کار
    with open(path, "r") as f:
        
        # خواندن محتوای فایل YAML و تبدیل آن به دیکشنری Python
        # استفاده از yaml.safe_load برای ایمنی در برابر اجرای کدهای مخرب
        return yaml.safe_load(f)

# تعریف تابعی به نام get_spark با سه پارامتر:
# app_name: نام اپلیکیشن اسپارک
# master: آدرس مستر اسپارک (مثلا local[*] یا yarn)
# shuffle_partitions: تعداد پارتیشن‌های شافل (پیش‌فرض=4)
def get_spark(app_name, master, shuffle_partitions=4):
    
    # ایجاد یک نمونه از SparkSession با استفاده از الگوی Builder
    spark = (SparkSession.builder
             
             # تنظیم نام اپلیکیشن اسپارک
             .appName(app_name)
             
             # تنظیم آدرس مستر اسپارک
             .master(master)
             
             # تنظیم تعداد پارتیشن‌های شافل برای عملیات shuffle
             # این تنظیم بر عملکرد join، groupBy و سایر عملیات تأثیر می‌گذارد
             .config("spark.sql.shuffle.partitions", shuffle_partitions)
             
             # ایجاد session یا استفاده از session موجود اگر قبلاً ایجاد شده
             .getOrCreate())
    
    # تنظیم سطح لاگ اسپارک به WARN برای کاهش لاگ‌های غیرضروری
    spark.sparkContext.setLogLevel("WARN")
    
    # بازگرداندن شیء SparkSession ایجاد شده
    return spark