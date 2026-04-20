import polars as pl
import asyncio
import random
from collections import Counter
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

MAX_CONCURRENT_TABS = 3  
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
INPUT_FILE = "test_link.xlsx"   
OUTPUT_FILE = "output_images.xlsx" 
IS_DEEP_SCAN = True 

async def get_images_from_page(page, url):
    """Lõi: Trích xuất ảnh từ một trang, tích hợp cuộn trang né Lazy Load"""
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=300000)

        for _ in range(3):
            await page.evaluate("window.scrollBy(0, window.innerHeight)")
            await asyncio.sleep(random.uniform(0.5, 1.2))
        
        # Đợi mạng ổn định
        await page.wait_for_load_state("networkidle", timeout=10000)

        img_links = await page.locator("img").evaluate_all(
            """imgs => imgs.map(img => img.src || img.getAttribute('data-src') || img.srcset)
                       .filter(src => src && src.startsWith('http'))"""
        )
        return list(set(img_links))
    except Exception as e:
        print(f"  [!] Bỏ qua {url} (Lỗi: Lâu phản hồi hoặc từ chối kết nối)")
        return []

async def crawl_internal_links(page, root_url):
    """Tìm toàn bộ link bài viết trên trang chủ (Yêu cầu 2)"""
    try:
        print(f"\n[>] Đang phân tích cấu trúc trang chủ: {root_url}")
        await page.goto(root_url, wait_until="networkidle", timeout=30000)
        
        links = await page.locator("a").evaluate_all(
            f"elements => elements.map(a => a.href).filter(href => href.includes('{root_url}') && !href.includes('#'))"
        )
        return list(set(links))
    except Exception as e:
        print(f"  [!] Không thể quét trang chủ {root_url}")
        return []

async def process_task(browser_context, url, semaphore):
    """Điều phối logic: Chạy Link đơn hoặc Quét toàn website"""
    async with semaphore:
        main_page = await browser_context.new_page()
        all_images = []
        
        if IS_DEEP_SCAN:
            sub_links = await crawl_internal_links(main_page, url)
            filtered_links = [link for link in sub_links if len(link) > 25]
            print(f"  [*] Tìm thấy {len(filtered_links)} URL hợp lệ cho {url}")
            await main_page.close()
            sub_semaphore = asyncio.Semaphore(3) 
            
            async def fetch_sub_page(sub_url):
                async with sub_semaphore:
                    sub_page = await browser_context.new_page()
                    imgs = await get_images_from_page(sub_page, sub_url)
                    await sub_page.close()
                    return list(set(imgs))

            sub_tasks = [fetch_sub_page(sub_url) for sub_url in filtered_links]
            sub_results = await asyncio.gather(*sub_tasks)
            all_images_flat = []
            for img_list in sub_results:
                all_images_flat.extend(img_list)
                
            img_counts = Counter(all_images_flat)
            unique_content_images = [img for img, count in img_counts.items() if count == 1]
            
            all_images.extend(unique_content_images)
            
        else:
            print(f"\n[>] Đang xử lý link đơn: {url}")
            imgs = await get_images_from_page(main_page, url)
            all_images.extend(imgs)
            await main_page.close()
            
        return {"Nguồn": url, "Ảnh": list(set(all_images))}

async def main():
    print("🚀 Khởi động Hệ thống The Stealthy Image Harvester...")

    try:
        df = pl.read_excel(INPUT_FILE)
        urls = df.get_column(df.columns[1]).drop_nulls().cast(pl.String).to_list()
        print(f"Đã tải {len(urls)} URLs từ {INPUT_FILE}")
        print(f"Chế độ quét: {'Deep Scan (Cả Website)' if IS_DEEP_SCAN else 'Single Link (Link đơn lẻ)'}")
    except Exception as e:
        print(f"LỖI ĐỌC FILE: {e}")
        return

    # --- BƯỚC 2: KHỞI ĐỘNG ĐỘNG CƠ TÀNG HÌNH ---
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)
    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)
        
        # Chạy tác vụ đa luồng
        tasks = [process_task(context, url, semaphore) for url in urls]
        results = await asyncio.gather(*tasks)
        
        await browser.close()

    print("\nĐang dàn trang và tổng hợp dữ liệu xuất file...")
    
    max_images = max((len(res["Ảnh"]) for res in results), default=0)
    
    data_dict = {"URL_Gốc": [res["Nguồn"] for res in results]}
    for i in range(max_images):
        data_dict[f"Link_Ảnh_{i+1}"] = [
            res["Ảnh"][i] if i < len(res["Ảnh"]) else None for res in results
        ]
        
    final_df = pl.DataFrame(data_dict)
    final_df.write_excel(OUTPUT_FILE)
    
    print(f"HOÀN TẤT! Dữ liệu của bạn đã sẵn sàng tại: {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())