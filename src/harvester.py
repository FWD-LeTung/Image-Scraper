import polars as pl
import asyncio
import random
import sys
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

MAX_CONCURRENT_TABS = 3  
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
INPUT_FILE = "test_link.xlsx"  
OUTPUT_FILE = "output_images.xlsx" 
IS_DEEP_SCAN = False


async def get_images_from_page(page, url):
    """Bóc tách link ảnh từ một trang cụ thể, xử lý cuộn trang (Lazy Load)"""
    try:
        print(f"  [+] Đang bóc tách ảnh: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        for _ in range(3):
            await page.evaluate("window.scrollBy(0, window.innerHeight)")
            await asyncio.sleep(random.uniform(0.5, 1.2)) 

        await page.wait_for_load_state("networkidle", timeout=10000)
        img_links = await page.locator("img").evaluate_all(
            """imgs => imgs.map(img => img.src || img.getAttribute('data-src') || img.srcset)
                       .filter(src => src && src.startsWith('http'))"""
        )
        return list(set(img_links)) 
    except Exception as e:
        print(f"  [!] Lỗi bóc tách tại {url}: {e}")
        return []

async def crawl_internal_links(page, root_url):
    """Quét trang chủ để lấy danh sách link bài viết (Dành cho Yêu cầu 2)"""
    try:
        print(f"  [>] Đang quét trang chủ: {root_url}")
        await page.goto(root_url, wait_until="networkidle", timeout=30000)

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
    print("Khởi động Hệ thống The Stealthy Image Harvester...")

    try:
        df = pl.read_excel(INPUT_FILE)
        url_column_index = 1 
        urls = df.get_column(df.columns[url_column_index]).drop_nulls().to_list()
        print(f"📦 Đã tải {len(urls)} URLs từ {INPUT_FILE}")
    except Exception as e:
        print(f"❌ LỖI ĐỌC FILE: {e}")
        return
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)

    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)
        
        tasks = [process_task(context, url, semaphore) for url in urls]
        results = await asyncio.gather(*tasks)
        
        await browser.close()

    print(" Đang dàn trang và tổng hợp dữ liệu xuất file...")

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