import os
import pandas as pd
import scrapy
from scrapy.cmdline import execute
from parsel import Selector
import re
from datetime import datetime
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor

def remove_extra_space(row_data):
    # Remove any extra spaces or newlines created by this replacement
    value = re.sub(r'\s+', ' ', row_data).strip()
    # Update the cleaned value back in row_data
    return value


def translate_text(text, source_lang, target_lang):
    """
    Translate a single text string using GoogleTranslator.
    """
    try:
        # Skip translation for NaN or empty values
        if pd.isna(text) or text == "":
            return text
        return GoogleTranslator(source=source_lang, target=target_lang).translate(text)
    except Exception as e:
        print(f"Error translating '{text}': {e}")
        return text  # Return the original text in case of error


def translate_dataframe(df, source_lang, target_lang, max_workers=5):
    """
    Translate all rows and columns in a DataFrame using multithreading.
    """
    translated_data = pd.DataFrame()

    # Multithreading for faster translation
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for column in df.columns:
            # Translate each column independently
            print(f"Translating column: {column}")
            translated_column = list(executor.map(
                lambda x: translate_text(x, source_lang, target_lang),
                df[column]
            ))
            translated_data[column] = translated_column

    return translated_data

def name_fun(selector):
    return remove_extra_space(selector.xpath('//div[@class="descDetaliiDisparuti"]/h3/text()').get('N/A'))

def date_of_birth_fun(selector):
    date_of_birth = selector.xpath('//div[@class="descDetaliiDisparuti"]/*[contains(text(),"Date of birth")]/text()').get()
    try:
        date_of_birth = remove_extra_space(' '.join(date_of_birth.split('Date of birth:')[1:]))
        date_of_birth = datetime.strptime(date_of_birth, '%d-%m-%Y').strftime('%d/%m/%Y')
    except:
        date_of_birth = 'N/A'
    return date_of_birth

def citizenship_fun(selector):
    citizenship = selector.xpath('//div[@class="descDetaliiDisparuti"]//*[contains(text(),"Citizenship")]/text()').get()
    try:
        citizenship = remove_extra_space(' '.join(citizenship.split('Citizenship:')))
    except:
        citizenship = 'N/A'
    return citizenship

def home_address_fun(selector):
    address = selector.xpath('//div[@class="descDetaliiDisparuti"]//*[contains(text(),"Home address")]/text()').get('N/A')
    try:
        address = remove_extra_space(' '.join(address.split('Home address:')[1:]))
    except:
        address = 'N/A'
    return address

def reason_fun(selector):
    reason = selector.xpath('//div[@class="descDetaliiDisparuti"]//*[contains(text(), "Reason")]/text()').get('N/A')
    try:
        reason = remove_extra_space(' '.join(reason.split('Reason:')[1:]))
    except:
        reason = 'N/A'
    return reason

def details_fun(selector):
    details = selector.xpath('//div[@class="detaliiSuplimentareDisparuti"]//p/text()').get('N/A')
    try:
        details = remove_extra_space(details)
    except:
        details = 'N/A'
    return details

def born_in_fun(selector):
    born_in = selector.xpath('//div[@class="descDetaliiDisparuti"]//*[contains(text(), "Born in")]/text()').get('N/A')
    try:
        born_in = remove_extra_space(' '.join(born_in.split('Born in:')[1:]))
    except:
        born_in = 'N/A'
    return born_in

def image_url_fun(selector):
    image_url = selector.xpath('//*[@class="pozaDetaliiDisparuti"]/img/@src').get('N/A')
    return image_url

class GovPolitiaromanaRoSpider(scrapy.Spider):
    name = "gov_politiaromana_ro"
    start_urls = ["https://politiaromana.ro/en/most-wanted"]
    final_data = list()

    cookies = {
        'cookiesession1': '678B286D4C4FAC37189C52B1F29297E0',
        'PHPSESSID': 'q3h6733mieu1efthfd12dm4jo2',
        'popup_login': 'yes',
        '_gid': 'GA1.2.982851257.1733120151',
        '_ga_NVWBC6YDH5': 'GS1.1.1733120151.2.1.1733120284.0.0.0',
        '_ga': 'GA1.1.1638476436.1732855986',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,tr;q=0.8',
        'cache-control': 'no-cache',
        # 'cookie': 'cookiesession1=678B286D4C4FAC37189C52B1F29297E0; PHPSESSID=q3h6733mieu1efthfd12dm4jo2; popup_login=yes; _gid=GA1.2.982851257.1733120151; _ga_NVWBC6YDH5=GS1.1.1733120151.2.1.1733120284.0.0.0; _ga=GA1.1.1638476436.1732855986',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    }

    indx = 0

    def start_requests(self):
        yield scrapy.Request(
            self.start_urls[0],
            headers=self.headers,
            cookies=self.cookies
        )

    def parse(self, response, **kwargs):
        selector = Selector(response.text)

        list_of_most_wanted = selector.xpath('//h3[@class="descNume"]/a/@href').getall()
        for url_of_most_wanted in list_of_most_wanted:
            yield scrapy.Request(
                url_of_most_wanted,
                headers=self.headers,
                cookies=self.cookies,
                callback=self.get_most_wanted_details
            )

        next_page_link = selector.xpath('//a[@class="buttonPaginatie next"]/@href').get()
        if next_page_link is not None:
            yield scrapy.Request(
                next_page_link,
                headers=self.headers,
                cookies=self.cookies,
                callback=self.parse
            )

    def get_most_wanted_details(self, response):
        main_dict = {}
        selector = Selector(response.text)

        main_dict['url'] = response.url

        main_dict['name'] = name_fun(selector)

        main_dict['date_of_birth'] = date_of_birth_fun(selector)

        main_dict['citizenship'] = citizenship_fun(selector)

        main_dict['address'] = home_address_fun(selector)

        main_dict['reason'] = reason_fun(selector)

        main_dict['details'] = details_fun(selector)

        main_dict['born_in'] = born_in_fun(selector)

        main_dict['image_url'] = image_url_fun(selector)

        # self.indx += 1
        # main_dict['id'] = self.indx

        self.final_data.append(main_dict)

    def close(self, spider, reason: str):
        df = pd.DataFrame(self.final_data)
        os.chdir(os.getcwd())
        if not os.path.exists('files'):
            os.makedirs('files')

        # Replace blank entries or 'None' with 'N/A'
        df = df.replace('', 'N/A').replace('None', 'N/A')  # Replace empty strings
        df.fillna('N/A', inplace=True)  # Replace None or NaN with 'N/A'

        df.insert(0, 'id', range(1, len(df) + 1))
        # sort the columns in a DataFrame
        sorted_columns = df.columns.sort_values()
        df = df.reindex(columns=sorted_columns)

        # Sort the columns, prioritizing specific ones like "url", "description", "supplier_summary"
        priority_columns = ["id", "url", "name"]
        df = df[priority_columns + [col for col in df.columns if col not in priority_columns]]
        input_file_path = os.getcwd() + f"\\files\\politiaromana_{datetime.now().strftime('%Y%m%d')}.xlsx"

        # Export the DataFrame to Excel
        df.to_excel(input_file_path, index=False, engine='openpyxl')

        source_language = "auto"  # Detect language automatically
        target_language = "en"
        translated_df = translate_dataframe(df, source_language, target_language, max_workers=10)

        translated_df = translated_df.replace('', 'N/A').replace('None', 'N/A')  # Replace empty strings
        translated_df.fillna('N/A', inplace=True)  # Replace None or NaN with 'N/A'

        # sort the columns in a DataFrame
        sorted_columns = translated_df.columns.sort_values()
        translated_df = translated_df.reindex(columns=sorted_columns)

        # Sort the columns, prioritizing specific ones like "url", "description", "supplier_summary"
        priority_columns = ["url", "name"]
        translated_df = translated_df[priority_columns + [col for col in translated_df.columns if col not in priority_columns]]
        input_file_path = os.getcwd() + f"\\files\\translated_politiaromana_{datetime.now().strftime('%Y%m%d')}.xlsx"

        # Export the DataFrame to Excel
        translated_df.to_excel(input_file_path, index=False, engine='openpyxl')

if __name__ == '__main__':
    # execute("scrapy crawl kia".split())
    execute("scrapy crawl gov_politiaromana_ro".split())
