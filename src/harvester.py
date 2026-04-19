import polars as pl
import asyncio
import random
import sys
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# ================= CẤU HÌNH HỆ THỐNG =================
MAX_CONCURRENT_TABS = 3  # Số lượng tab chạy song song (Tối ưu: 3-5)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
INPUT_FILE = "test_link.xlsx"  # Tên file đầu vào
OUTPUT_FILE = "output_images.xlsx" # Tên file kết quả
IS_DEEP_SCAN = False
# =======================================================

async def get_images_from_page(page, url):
    """Bóc tách link ảnh từ một trang cụ thể, xử lý cuộn trang (Lazy Load)"""
    try:
        print(f"  [+] Đang bóc tách ảnh: {url}")
        # Đợi DOM tải xong, không cần đợi toàn bộ tài nguyên để tiết kiệm thời gian
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        
        # Chiến thuật cuộn trang: Cuộn từ từ để kích hoạt mã JS của Lazy Load
        for _ in range(3):
            await page.evaluate("window.scrollBy(0, window.innerHeight)")
            await asyncio.sleep(random.uniform(0.5, 1.2)) # Nghỉ nhịp giống người thật
        
        # Đợi mạng ổn định sau khi cuộn
        await page.wait_for_load_state("networkidle", timeout=10000)
        
        # Bóc tách: Tìm trong src, data-src (lazy load), hoặc srcset
        img_links = await page.locator("img").evaluate_all(
            """imgs => imgs.map(img => img.src || img.getAttribute('data-src') || img.srcset)
                       .filter(src => src && src.startsWith('http'))"""
        )
        return list(set(img_links)) # set() để loại bỏ các link trùng lặp
    except Exception as e:
        print(f"  [!] Lỗi bóc tách tại {url}: {e}")
        return []

async def crawl_internal_links(page, root_url):
    """Quét trang chủ để lấy danh sách link bài viết (Dành cho Yêu cầu 2)"""
    try:
        print(f"  [>] Đang quét trang chủ: {root_url}")
        await page.goto(root_url, wait_until="networkidle", timeout=30000)
        
        # Tìm các link cùng domain, loại bỏ anchor link (#)
        links = await page.locator("a").evaluate_all(
            f"elements => elements.map(a => a.href).filter(href => href.includes('{root_url}') && !href.includes('#'))"
        )
        return list(set(links))
    except Exception as e:
        print(f"  [!] Lỗi quét trang chủ {root_url}: {e}")
        return []

async def process_task(browser_context, url, semaphore):
    """Điều phối luồng xử lý cho từng URL gốc"""
    async with semaphore:
        # Tạo tab mới (Đã được tàng hình từ Context)
        page = await browser_context.new_page()
        all_images = []
        
        if IS_DEEP_SCAN:
            sub_links = await crawl_internal_links(page, url)
            print(f"  [*] Tìm thấy {len(sub_links)} link bài viết cho {url}")
            # Giới hạn quét 5 bài viết đầu tiên để test. Bạn có thể xóa [:5] khi chạy thật
            for sub_url in sub_links[:5]: 
                imgs = await get_images_from_page(page, sub_url)
                all_images.extend(imgs)
        else:
            imgs = await get_images_from_page(page, url)
            all_images.extend(imgs)
            
        await page.close()
        return {"Nguồn": url, "Ảnh": list(set(all_images))}

async def main():
    print("🚀 Khởi động Hệ thống The Stealthy Image Harvester...")
    
    # 1. ĐỌC DỮ LIỆU ĐẦU VÀO BẰNG POLARS
    try:
        # Ép Polars đọc tất cả dưới dạng String ngay từ đầu để triệt tiêu cảnh báo (Warning)
        df = pl.read_excel(INPUT_FILE)
        # Lấy URL từ cột B (Index 1) theo cấu trúc file test của bạn (Cột A là STT)
        url_column_index = 1 
        urls = df.get_column(df.columns[url_column_index]).drop_nulls().to_list()
        print(f"📦 Đã tải {len(urls)} URLs từ {INPUT_FILE}")
    except Exception as e:
        print(f"❌ LỖI ĐỌC FILE: {e}")
        return

    # 2. KHỞI CHẠY ENGINE CÀO DỮ LIỆU
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)
    
    # Bọc tàng hình (Stealth) ở cấp độ cốt lõi của thư viện
    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)
        
        # Chạy tác vụ song song
        tasks = [process_task(context, url, semaphore) for url in urls]
        results = await asyncio.gather(*tasks)
        
        await browser.close()

    # 3. XỬ LÝ KẾT QUẢ VÀ XUẤT FILE
    print("💾 Đang dàn trang và tổng hợp dữ liệu xuất file...")
    
    # Tìm bài viết có nhiều ảnh nhất để tạo số lượng cột tương ứng
    max_images = max((len(res["Ảnh"]) for res in results), default=0)
    
    # Dựng cấu trúc Dictionary để Polars tạo DataFrame
    data_dict = {"URL_Gốc": [res["Nguồn"] for res in results]}
    for i in range(max_images):
        data_dict[f"Link_Ảnh_{i+1}"] = [
            res["Ảnh"][i] if i < len(res["Ảnh"]) else None for res in results
        ]
        
    final_df = pl.DataFrame(data_dict)
    final_df.write_excel(OUTPUT_FILE)
    
    print(f"✅ HOÀN TẤT! Dữ liệu của bạn đã sẵn sàng tại: {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())