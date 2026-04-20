# ImageHarvester - Hướng dẫn cài đặt và sử dụng

## Cài đặt

### Bước 1: Cài đặt Python
- Tải và cài đặt Python 3.13 từ: https://www.python.org/downloads/
- **Quan trọng:** Khi cài đặt, tích chọn "Add Python to PATH"

### Bước 2: Cài đặt Playwright Driver
- Chạy file `install_driver.bat` (nhấp đúp vào file)
- Đợi quá trình cài đặt hoàn tất (khoảng 5-10 phút)
- Hoặc chạy lệnh thủ công:
  ```
  pip install playwright
  playwright install firefox
  ```

## Sử dụng

1. Nhấp đúp vào file `ImageHarvester.exe` để mở ứng dụng
2. Chọn file dữ liệu (Excel/CSV) chứa các link cần quét
3. Bật/tắt chế độ Deep Scan:
   - **Tắt (mặc định):** Quét ảnh từ trang hiện tại
   - **Bật:** Quét sâu vào các link nội bộ (dùng cho trang chủ)
4. Nhấn "BẮT ĐẦU THU HOẠCH" để chạy

## Yêu cầu hệ thống
- Windows 10/11
- Python 3.13 (đã cài PATH)
- Kết nối internet

## Lưu ý
- File kết quả sẽ được lưu cùng thư mục với file đầu vào, thêm hậu tố `_KET_QUA.xlsx`
- Có thể tạm thời tắt antivirus nếu exe bị chặn
