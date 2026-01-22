from types import SimpleNamespace
import requests
from google import genai
import mysql.connector
from dotenv import load_dotenv
from jinja2 import Template
from pathlib import Path
from bs4 import BeautifulSoup
import re
import json
from urllib.parse import urljoin, urlparse
from collections import deque
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import base64
from email.message import EmailMessage
from groq import Groq
from ollama import Client as OlClient
import cohere as coclient
import os

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
creds = Credentials.from_authorized_user_file("token.json", SCOPES)
service = build("gmail", "v1", credentials=creds)

load_dotenv()

query = "SELECT * FROM stack WHERE exec != 'running' LIMIT 1"

conn = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    database= os.getenv("DB_NAME"),
    port=3306
)

gemini = genai.Client()
groq = Groq()
ollama = OlClient(os.getenv('OLLAMA_HOST'))
cohere = coclient.ClientV2(os.getenv('COHERE_API_KEY'))
cursor = conn.cursor(dictionary=True)
row = None
def main():
    while True:
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.commit() 
        print(rows)
        if not rows:
            print("breaking...")
            break
        global row
        row = rows[0]
        descr = row["descr"]
        jobtype = row["type"]

        def llm(*, system= None, user = None):
            models = [("gemini", "gemini-2.5-flash"),("groq", "openai/gpt-oss-120b"),("ollama", "deepseek-v3.1:671b-cloud"),("cohere","command-a-03-2025")]
            messages = []
            gemmsgs = []
            if system is None:
                messages = [{"role": "user", "content": user}]
                gemmsgs = [user]
            else:
                messages = [{"role": "system", "content": system},{"role": "user", "content": user}]
                gemmsgs = [system,user]

            for provider,model in models:
                print()
                try:
                    if provider == "groq":
                        groqurl = groq.chat.completions.create(model=model,messages = messages)
                        print(provider)
                        return groqurl.choices[0].message.content

                    if provider == "ollama":
                        olurl = ollama.chat(model= model, messages = messages)
                        print(provider)
                        return olurl.message.content

                    if provider == "cohere":
                        courl = cohere.chat(model=model,messages=messages)
                        print(provider)
                        return courl

                    if provider == "gemini":
                        gemurl = gemini.models.generate_content(model=model, contents=gemmsgs)
                        print(provider)
                        return gemurl.text
                except Exception as e:
                    continue

                raise RuntimeError("All llms failed")
        
        tempurl = Template(Path("prompturl.jinja").read_text())
        prompturl = tempurl.render()
        aiurl = llm(system = prompturl,user = descr+jobtype)
        print(aiurl)
        result = {}
        loop = False
        q = deque()
        q.append(aiurl)
        allemails=set()
        count =1
        aimsg=None
        aititle=None
        if "null" not in aiurl.lower():
            while count <=5:
                count +=1
                if not q:
                    break
                jinaurl = f"http://51.21.203.243:3000/{q.popleft()}"
                print(jinaurl)
                headers = {
                    "X-Engine": "direct",
                    "X-Return-Format": "html"
                }
                response = requests.get(jinaurl, headers=headers)
                html_content = response.text
                url = aiurl

                EXCLUDED_DOMAINS = [
                    "github.com", "githubassets.com", "wixpress.com", "mailchimp.com", "wordpress.com",
                    "domain.com", "no-reply.com", "example.com", "sentry.io", "sentry.com", "figma.com", "mysite"
                ]

                LANGUAGE_KEYWORDS = {
                    'contact': ['contact', 'contacto', 'kontakt', 'контакт', 'contatto', 'kontakt'],
                    'about': ['about', 'acerca de', 'propos', 'über', 'informazioni', 'sobre', 'nas', 'over',
                            'support', 'get-to-know-us', 'quiénes-somos', 'quienes-somos',
                            'get in touch', 'getintouch', 'get-in-touch']
                }

                def is_valid_email(email):
                    email_regex = (
                        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.'
                        r'(com|edu|gov|org|in|co|net|biz|online|tech|info|io|ai|app|dev|xyz|store|design|us|uk|ca|au|de|fr|es|it|jp|cn|com\.in|co\.uk)'
                        r'(\.[a-zA-Z]{2,})?$'
                    )
                    return re.match(email_regex, email)

                def extract_emails(content):
                    raw_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', content)
                    valid_emails = set(email for email in raw_emails if is_valid_email(
                        email) and not any(domain in email for domain in EXCLUDED_DOMAINS))
                    mailto_emails = re.findall(
                        r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', content)
                    valid_emails.update(mailto_emails)
                    return list(valid_emails)

                def extract_phone_numbers(soup):
                    visible_text = soup.get_text(separator=" ", strip=True)
                    pattern = r'(?<!\w)(?:\+\d{1,3}[-.\s]?)?(?:\(\d{1,4}\)[-\s]?)?\d{2,4}[-.\s]?\d{2,4}[-.\s]?\d{2,9}(?!\w)'
                    return list(set(re.findall(pattern, visible_text)))

                def extract_social_links(base_url, soup):
                    social_links = {"instagram": [], "linkedin": [],
                                    "facebook": [], "twitter": [], "whatsapp": []}
                    for tag in soup.find_all('a', href=True):
                        link = urljoin(base_url, tag['href'])
                        domain = urlparse(link).netloc.lower()
                        path = urlparse(link).path.lower()

                        if "instagram.com" in domain and "/p/" not in path:
                            social_links["instagram"].append(link)
                        elif "linkedin.com" in domain and ("/in/" in path or "/company/" in path):
                            social_links["linkedin"].append(link)
                        elif "facebook.com" in domain and not any(x in path for x in ["/share/", "/photo/", "/video/"]):
                            social_links["facebook"].append(link)
                        elif "twitter.com" in domain or "x.com" in domain and "/status/" not in path:
                            social_links["twitter"].append(link)
                        elif "wa.me" in domain or "whatsapp.com" in domain:
                            social_links["whatsapp"].append(link)
                    return social_links

                def get_relevant_links(base_url, soup):
                    base_domain = urlparse(base_url).netloc
                    relevant_links = set()
                    for tag in soup.find_all('a', href=True):
                        if len(relevant_links) >= 2:
                            break
                        link = urljoin(base_url, tag['href'])
                        link_path = urlparse(link).path.lower()
                        if urlparse(link).netloc == base_domain:
                            if any(k in link_path for k in LANGUAGE_KEYWORDS['contact']) or \
                                    any(k in link_path for k in LANGUAGE_KEYWORDS['about']):
                                relevant_links.add(link)
                    return list(relevant_links)

                def extract_company_names(soup):
                    company_names = set()
                    for meta in soup.find_all('meta', attrs={'property': 'og:site_name'}):
                        if meta.get('content'):
                            company_names.add(meta['content'])
                    for meta in soup.find_all('meta', attrs={'name': 'og:company'}):
                        if meta.get('content'):
                            company_names.add(meta['content'])
                    title_tag = soup.find('title')
                    if title_tag:
                        company_names.add(title_tag.get_text().strip())
                    for script in soup.find_all('script', type='application/ld+json'):
                        try:
                            data = json.loads(script.string)
                            if isinstance(data, dict) and data.get('@type') == 'Organization' and 'name' in data:
                                company_names.add(data['name'])
                        except:
                            continue
                    for img in soup.find_all('img', alt=True):
                        if 'logo' in img['alt'].lower():
                            company_names.add(img['alt'].strip())
                    return list(company_names)

                def extract_domain_parts(url):
                    netloc = urlparse(url).netloc
                    parts = netloc.split('.')
                    if parts and parts[0].lower() == 'www':
                        parts = parts[1:]
                    return parts

                def check_common_substring(company_names, domain_parts):
                    all_collections = []
                    longest_collection = ""
                    process_result = []
                    for domain_part in domain_parts:
                        dp = domain_part.lower()
                        for name in company_names:
                            words = name.split()
                            current_collection = []
                            for i, word in enumerate(words):
                                if word.lower() in dp:
                                    current_collection.append(word)
                                    process_result.append(
                                        {"word": word, "status": "matched", "index": i, "domain_part": domain_part})
                                else:
                                    if current_collection:
                                        coll = " ".join(current_collection)
                                        all_collections.append(coll)
                                        if len(coll) > len(longest_collection):
                                            longest_collection = coll
                                        current_collection = []
                            if current_collection:
                                coll = " ".join(current_collection)
                                all_collections.append(coll)
                                if len(coll) > len(longest_collection):
                                    longest_collection = coll
                    return {"all_collections": all_collections, "longest_collection": longest_collection, "process_result": process_result}

                def extract_company_sources(soup):
                    sources = {}
                    meta_tag = soup.find('meta', attrs={'property': 'og:site_name'})
                    if meta_tag and meta_tag.get('content'):
                        sources["meta"] = meta_tag['content']
                    title_tag = soup.find('title')
                    if title_tag:
                        sources["title_tag"] = title_tag.get_text().strip()
                    for script in soup.find_all('script', type='application/ld+json'):
                        try:
                            data = json.loads(script.string)
                            if isinstance(data, dict) and data.get('@type') == 'Organization' and 'name' in data:
                                sources["json_ld"] = data['name']
                        except:
                            continue
                    for img in soup.find_all('img', alt=True):
                        if 'logo' in img['alt'].lower():
                            sources["img"] = img['alt'].strip()
                    return sources

                soup = BeautifulSoup(html_content, 'html.parser')
                emails = extract_emails(html_content)
                phones = extract_phone_numbers(soup)
                social_links = extract_social_links(url, soup) if url else {}
                relevant_links = get_relevant_links(url, soup) if url else []
                company_names = extract_company_names(soup)
                sources = extract_company_sources(soup)
                domain_parts = extract_domain_parts(url) if url else []
                matching_results = check_common_substring(company_names, domain_parts) if url else {}

                if loop is False:
                    result = {
                        "emails": emails or [],
                        "phone_numbers": phones or [],
                        "social_links": social_links,
                        "whatsapp_links": social_links.get("whatsapp", []),
                        "relevant_links": relevant_links,
                        "sources": sources,
                        "common_name_part": matching_results.get("longest_collection", ""),
                        "domain_parts": domain_parts,
                        "matching_logs": matching_results.get("process_result", [])
                    }
                    print(json.dumps(result, indent=4))
                    q.extend(relevant_links)
                    loop = True

                print(f"Extracted emails: {emails}")
                if emails:
                    for email in emails:
                        allemails.add(email)

            print(f"\nAll extracted emails: {allemails}\n")

            tempmsg = Template(Path("promptmsg.jinja").read_text())
            promptmsg = tempmsg.render(desc= descr, type=jobtype, cnp = result["common_name_part"], dp = result["domain_parts"])
            aimsg = llm(user = promptmsg)
            # aimsg = SimpleNamespace(text="Hello Amica Early Learning, I saw your Upwork post...")

            temptitle = Template(Path("prompttitle.jinja").read_text())
            prompttitle = temptitle.render(desc=descr)
            aititle=llm(user=prompttitle)
            # aititle = SimpleNamespace(text="help with your Upwork post")

            if allemails:
                ecount=0
                for email in allemails:
                    ecount+=1
                    if ecount == 4:
                        break
                    msg = EmailMessage()
                    msg["To"] = email
                    msg["From"] = "me"
                    msg["Subject"] = f"I will {aititle}"
                    msg.set_content(aimsg)

                    encoded_msg = base64.urlsafe_b64encode(msg.as_bytes()).decode()
                    service.users().messages().send(userId="me", body={"raw": encoded_msg}).execute()
                    print(f"Email sent to: {email}")

        cursor.execute("DELETE FROM stack WHERE id = %s", (row["id"],))
        print(f"Deleted processed row with id: {row['id']}")
        cursor.execute("INSERT INTO upwork (`desc`, url, env, upwork_link, relevant_links, socials, email, application, title) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (descr, aiurl, jobtype, row["upwork"], json.dumps(result.get("relevant_links",[])), json.dumps(result.get("social_links",[])), json.dumps(list(allemails)), aimsg, aititle))
        conn.commit()

try:
    main()
except Exception as e:
    print (e)
    cursor.execute("UPDATE stack SET exec = 'running' WHERE id = %s", (row["id"],))
    print(cursor, "done", row["id"])
    conn.commit()
