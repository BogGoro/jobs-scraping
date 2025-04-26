import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Language': 'ru-RU,ru;q=0.9'
}

def fetch_vacancies(experience, pages=5):
    base_url = "https://hh.ru/search/vacancy"
    vacancies = []
    
    for page in range(pages):
        params = {
            'text': '"Data+Engineer"+OR+"Инженер+данных"+OR+"Data+инженер"',
            'experience': experience,
            'search_field': 'name',
            'page': page,
            'items_on_page': 20
        }
        
        try:
            response = requests.get(base_url, headers=HEADERS, params=params)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Check if no results found
            no_results = soup.find('h1', {'data-qa': 'title'})
            if no_results and 'ничего не найдено' in no_results.text:
                print(f"No results found for experience {experience}, page {page}")
                break
                
            # Find vacancy items - this selector might need adjustment
            items = soup.find_all('div', class_='serp-item')
            
            if not items:
                print("No items found - check HTML structure or if you're being blocked")
                break
                
            for item in items:
                try:
                    title = item.find('a', {'data-qa': 'serp-item__title'}).text.strip()
                    company = item.find('a', {'data-qa': 'vacancy-serp__vacancy-employer'}).text.strip()
                    skills = [skill.text.strip() for skill in item.find_all('div', {'data-qa': 'vacancy-serp__vacancy_snippet_requirement'})]
                    
                    vacancies.append({
                        'title': title,
                        'company': company,
                        'skills': ', '.join(skills),
                        'experience': experience
                    })
                except Exception as e:
                    print(f"Error parsing item: {e}")
                    continue
            
            print(f"Page {page+1} done for experience {experience}")
            time.sleep(random.uniform(1, 3))  # Be polite with delays
            
        except Exception as e:
            print(f"Error fetching page {page} for experience {experience}: {e}")
            break
            
    return pd.DataFrame(vacancies)

# Experience levels mapping
experience_levels = {
    'intern': 'noExperience',
    'junior': 'between1And3',
    'middle': 'between3And6',
    'senior': 'moreThan6'
}

# Scrape for each level
dfs = []
for level, exp_code in experience_levels.items():
    print(f"Scraping {level} positions...")
    df = fetch_vacancies(experience=exp_code, pages=10)  # Try 10 pages
    df['level'] = level
    dfs.append(df)
    print(f"Found {len(df)} {level} positions")

# Combine all data
all_vacancies = pd.concat(dfs, ignore_index=True)

# Save to CSV
all_vacancies.to_csv('vacancies.csv', index=False)
print("Scraping completed. Data saved to CSV.")
