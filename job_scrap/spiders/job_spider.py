import scrapy
import html
import logging
import re
import os
import concurrent.futures
import asyncio
import traceback
from urllib.parse import urljoin
import time
from scrapy.http import Request
from html import unescape
from scrapy.extensions import feedexport
from bs4 import BeautifulSoup
from csv_diff import load_csv, compare
import csv
import io
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import os
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP, SMTP_SSL
import json
import hashlib


header_keys=[
    'Key'                     ,
    'Order Location'          ,
    'Job Title'               ,
    'No of Vacancy'           ,
    'Monthly Salary'          ,
    'Business'                ,
    'Post-quali Exp Yr'       ,
    'Published Requirements'  ,
    'Last Update'             ,
    'Job Key No',
    'Job Title/ Category',
    'Number Of Vacancy',
    'Relevant Field',
    'Nature',
    'Contract Period',
    'Payroll',
    'Employer Business',
    'Location Base',
    'Monthly Salary Range HK$',
    'Duties',
    'Yrs of Total Post-Quali Exp',
    'Yrs of Relevant Exp',
    'Requirements',
    'Work Outside Current Location',
    'Last Update',
    'Apply To',
    'Direct Line',
    'URL'

]
_is_none_list = lambda x: x == [None]



class JobSpider(scrapy.Spider):
    name = 'job_spider'
    start_urls = ['https://www.infotech.com.hk/itjs/job/fe-search.do?method=feList']  # Update with the actual URL
    global logger
    global checkfile_path
    global bk_checkfile_path
    global chg_checkfile_path
    global job_data_list
    global smtp_hostname
    global smtp_port

    def __init__(self, *args, **kwargs):
        super(JobSpider, self).__init__(*args, **kwargs)
        global logger
        global checkfile_path
        global bk_checkfile_path
        global job_data_list
        global chg_checkfile_path
        global smtp_hostname
        global smtp_port

        # smtp_hostname='10.63.113.88'
        # smtp_port=25
        smtp_hostname='smtp.gmail.com'
        smtp_port=465
        sender_email = "weikwan214@gmail.com"
        sender_password = "kcfuiipahudapoem"  # Use App Password if 2FA is enabled

        job_data_list=[]


        # Get Date Time
        dt = datetime.now()
        input_dt_str = dt.strftime("%Y%m%d%H%M%S")


        # bk_checkfile_path=f'C:\workspace\cprs\mysql_excel_clear\job_scrap\job_ads_{input_dt_str}.csv'
        # checkfile_path='C:\workspace\cprs\mysql_excel_clear\job_scrap\job_ads.csv'

        script_name = os.path.basename(__name__)
        script_name_clean=os.path.splitext(os.path.basename(script_name))[0].split('.')[0]
        prepend_checkfile_path_dir = os.path.join(os.getcwd(),script_name_clean, f"{script_name_clean}_records")
        if not os.path.lexists(prepend_checkfile_path_dir) : os.makedirs(prepend_checkfile_path_dir, True)
        checkfile_path=os.path.join(prepend_checkfile_path_dir, 'job_ads.csv')
        checkfile_path_adv=os.path.join(prepend_checkfile_path_dir, 'job_advs.csv')
        checkfile_path_plain= os.path.splitext(os.path.basename(checkfile_path))[0]
        checkfile_path_adv_plain= os.path.splitext(os.path.basename(checkfile_path_adv))[0]
        bk_checkfile_path_adv=f'{os.path.basename(checkfile_path_adv_plain)}_{input_dt_str}.csv'

        bk_checkfile_path=os.path.join(prepend_checkfile_path_dir,f'{os.path.basename(checkfile_path_plain)}_{input_dt_str}.csv')
        chg_checkfile_path=os.path.join(prepend_checkfile_path_dir,f'{os.path.basename(checkfile_path_plain)}_change_{input_dt_str}.csv')
        checkfile_path_os=os.path.join(checkfile_path)

        isFileHere=os.path.isfile(checkfile_path_os)
        isFileExists=os.path.exists(checkfile_path_os)

        if isFileExists :
            os.rename(checkfile_path_os, bk_checkfile_path)

        if os.path.exists(checkfile_path_adv) :
            os.rename(checkfile_path_adv, bk_checkfile_path_adv)

        # self.original_file = 'output.csv'  # Change to your actual output file name
        # self.new_file = 'renamed_output.csv'  # The desired new file name
        logger=self.logger
        logger.info(f'self.custom_settings : {self.custom_settings}')
        logger.info(f'kwargs : {kwargs}')
        # exit(0)

    def extract_strings_in_brackets(self, text):
        # Regular expression to find all uppercase abbreviations of exactly three letters
        pattern = r'\((([A-Z]{3,5}))\)'
        pattern = r'\(([A-Z]{3,5})\)'

        matches = re.findall(pattern, text)
        return matches

    def extract_integer_after_dash(self, text):
        # Regular expression to find an integer after a dash
        pattern = r'-(\d+)'
        match = re.search(pattern, text)
        return match.group(1) if match else None


    # def send_email_with_restricted_marking_and_attachment(self, sender, team_name, recipients, cc_recipients, subject, body, attachment_path, isEncrypt=False,smtp_hostname=smtp_hostname, smtp_port=smtp_port):
    #     self.send_email_with_restricted_marking_and_attachment(
    #         sender,
    #         '',
    #         ['jkwei@try.gov.hk'],
    #         [],
    #         'Daily Tcon Change',
    #         '',
    #         attachment_path
    #     )
    def send_email_with_restricted_marking_and_attachment(self, sender, team_name, recipients, cc_recipients, subject, body, attachment_paths, isEncrypt=False,smtp_hostname='smtp.gmail.com', smtp_port=465):
        global logger
        # Create the email message
        try :
                msg = MIMEMultipart()
                msg['Subject'] = Header(subject.replace(":team_name", team_name), 'utf-8')
                msg['From'] = sender
                if isinstance(recipients,list) and not _is_none_list(recipients) : pass
                else : recipients=[""]
                if isinstance(cc_recipients,list) and not _is_none_list(cc_recipients) : pass
                else : cc_recipients=[""]
                msg['To'] = ', '.join(recipients)
                msg['Cc'] = ', '.join(cc_recipients)
                if isEncrypt : msg.add_header('X-Encrypted', 'yes')

                # Add the "Restricted" marking, no nd urgent
                # msg['X-Priority'] = '1'
                # msg['X-MSMail-Priority'] = 'High'
                # msg['Importance'] = 'High'
                msg['X-Sensitivity'] = 'Confidential'

                # Add the email body
                msg.attach(MIMEText(body))

                # Add the attachment
                if attachment_paths and isinstance(attachment_paths, list) and len(attachment_paths) > 0 :
                    for attachment_path in attachment_paths :
                        # logger.info(f"Attaching File to email {attachment_path}")
                        with open(attachment_path, "rb") as f:
                            attachment = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
                        attachment['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
                        msg.attach(attachment)

                # Send the email
                # with SMTP(smtp_hostname, smtp_port) as smtp:
                with SMTP_SSL(smtp_hostname, smtp_port, timeout=30) as smtp:
                    sender_password = "kcfuiipahudapoem"  # Use App Password if 2FA is enabled

                    smtp.login(sender, sender_password)
                    smtp.send_message(msg);logger.info(f"sent email to {recipients} and {cc_recipients}. Attached File to email {attachment_path}")

        except Exception as e:logger.error(traceback.format_exc());logger.error(e)

    def clean_text(self, text):
        """Remove newlines and tabs from text."""
        if text:
            tmp_text = text.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ').replace('\"', ' ')
            cleaned_text = ' '.join(tmp_text.split()).strip()
            # logging.debug(f'Cleaned text: {cleaned_text}')
            return cleaned_text
        return ''

    def clean_html_content(self, html_content):
        """Remove HTML tags and clean the text."""
        # Use html.unescape to handle HTML entities
        clean_text = html.unescape(html_content)
        # Remove HTML tags
        clean_text = html.unescape(clean_text).replace("<br>", "\n").strip()
        cleaned_lines = [re.sub(r'<td></td>', '',line.strip()) for line in clean_text.splitlines() if line.strip()]
        clean_text = " ".join(cleaned_lines)
        trimmed_html = re.sub(r'<td>(.*?)</td>', r'\1', clean_text)

        # logging.debug(f'Cleaned HTML content: {trimmed_html}')
        # return trimmed_html 
        return trimmed_html if trimmed_html.startswith('\"') else f'"{trimmed_html}"'

    def blocking_task(self, item):
        """Simulate a blocking task (e.g., CPU-bound operation)."""
        logger.info(f"Processing {item}")
        # Simulate a delay for the blocking task
        time.sleep(1)  # Blocking call
        return f"Processed {item}"

    def display_results(self):
        """Display the processed results."""
        for result in self.results:
            print(result)

    async def run_blocking_tasks(self, items):
        """Run blocking tasks in a thread pool."""
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            tasks = [
                loop.run_in_executor(executor, self.blocking_task, item) for item in items
            ]
            self.results = await asyncio.gather(*tasks)

    async def run_and_display(self, items):
        """Run the blocking tasks and display results."""
        await self.run_blocking_tasks(items)
        self.display_results()

    def plainTextContent(self, html_content) :
        # Step 1: Unescape HTML entities
        plain_text = unescape(html_content)

        # Step 2: Replace <br> with newline and clean up
        plain_text = plain_text.replace('<br>', ';')

        # Step 3: Remove HTML tags
        plain_text = re.sub(r'<[^>]*>', '', plain_text)

        # Step 4: Replace multiple whitespace characters with a single space
        plain_text = re.sub(r'\s+', ' ', plain_text).strip()

        # Result
        # logger.debug(plain_text)
        return plain_text

    all_skills_full_list:list=[]
    def parseDetails(self, response):
        # global last_url
        # logger.debug(f'parseDetails ... {response.url} . {last_url}')
        import scrapy.http.response.html
        logger.debug(f'parseDetails ... {response.url}.')

        try :
            job_key=response.xpath('//td[contains(text(), "Job Key No")]/following-sibling::td/b/text()').get(default='').strip()
            logger.debug(f'job_key is : {job_key}')
            if job_key and isinstance(job_key, str) :
                job_title_category=response.xpath('//td[contains(text(), "Job Title/ Category")]/following-sibling::td/b/text()').get(default='').strip()
                ProjectNature= response.xpath('//td[contains(text(), "Project Nature")]/following-sibling::td/text()').get(default='')
                plain_ProjectNature=self.plainTextContent(ProjectNature)
                plain_ProjectNature_str=f'"{plain_ProjectNature.replace('"','""')}"'
                Duties_str= response.xpath('//td[contains(text(), "Duties")]/following-sibling::td').get(default='')
                plain_Duties=self.plainTextContent(Duties_str)
                plain_Duties_str=f'"{plain_Duties.replace('"','""')}"'
                Requirements=response.xpath('//td[contains(text(), "Requirements")]/following-sibling::td').get(default='')
                plain_Requirements=self.plainTextContent(Requirements)
                plain_Requirements_str=f'"{plain_Requirements.replace('"','""')}"'
                # skills=','.join(self.extract_strings_in_brackets(plain_Requirements_str))
                tmp_skills=self.extract_strings_in_brackets(plain_Requirements_str)
                Relevant_Field_str=response.xpath('//td[contains(text(), "Relevant Field")]/following-sibling::td/text()').get(default='').strip()


                skills=tmp_skills
                skills.sort()
                self.all_skills_full_list.append(skills)
                hyperlink=f'=Hyperlink("{response.url}")'
                op_data = {

    'Job Key No'					: job_key,
    'Job Title/ Category'			: job_title_category,
    'Post Times'        			: self.extract_integer_after_dash(job_title_category),
    'Number Of Vacancy'				: response.xpath('//td[contains(text(), "Number Of Vacancy")]/following-sibling::td/text()').get(default='').strip(),
    'Relevant Field'				: Relevant_Field_str,
    'Nature'						: response.xpath('//td[contains(text(), "Nature")]/following-sibling::td/text()').get(default='').strip(),
    'Deadline'						: response.xpath('//td[contains(text(), "Deadline")]/following-sibling::td/text()').get(default='').strip(),
    'Contract Period'				: response.xpath('//td[contains(text(), "Contract Period")]/following-sibling::td/text()').get(default='').strip(),
    'Payroll'						: response.xpath('//td[contains(text(), "Payroll")]/following-sibling::td/text()').get(default='').strip(),
    'Employer Business'				: response.xpath('//td[contains(text(), "Employer Business")]/following-sibling::td/text()').get(default='').strip(),
    'Location Base'					: response.xpath('//td[contains(text(), "Location Base")]/following-sibling::td/text()').get(default='').strip(),
    'Monthly Salary Range HK$'		: response.xpath('//td[contains(text(), "Monthly Salary Range HK$")]/following-sibling::td/text()').get(default='').strip(),
    'Project Nature'				: plain_ProjectNature_str,
    'Duties'						: plain_Duties_str,
    'Yrs of Total Post-Quali Exp'	: response.xpath('//td[contains(text(), "Yrs of Total Post-Quali Exp")]/following-sibling::td/text()').get(default='').strip(),
    'Yrs of Relevant Exp'			: response.xpath('//td[contains(text(), "Yrs of Relevant Exp")]/following-sibling::td/text()').get(default='').strip(),
    'Requirements'					: plain_Requirements_str,
    'Work Outside Current Location'	: response.xpath('//td[contains(text(), "Work Outside Current Location")]/following-sibling::td/text()').get(default='').strip(),
    'Last Update'					: response.xpath('//td[contains(text(), "Last Update")]/following-sibling::td/text()').get(default='').strip(),
    'Apply To'						: response.xpath('//td[contains(text(), "Apply To")]/following-sibling::td/text()').get(default='').strip(),
    'Direct Line'					: response.xpath('//td[contains(text(), "Direct Line")]/following-sibling::td/text()').get(default='').strip(),
    'URL'       					: hyperlink,
    # 'skills'       					: skills,

                }

                [op_data.update({header_skills_list_itm : header_skills_list_itm}) for header_skills_list_itm in skills]
                # len_plain_Duties=len(plain_Duties_str)
                # if len_plain_Duties < 256 : logger.info(f'plain_Duties text ***************** : {plain_Duties_str}')

                if job_title_category and isinstance(job_title_category, str) :
                    job_data_list.append(op_data)
                    yield op_data  # Yield the dictionary with all td values
                else :
                    logger.info(f'job_key {job_key} is not completed.')

        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(e)

    async def final_tasks(self):
        global logger
        # Perform any final asynchronous tasks here
        logger.info("Running final tasks after the spider has closed...")
        # self.complete_all()
        # Example: await some asynchronous operation

    def close(self, reason):
        global logger
        # This method is called when the spider is closed
        logger.info(f"Spider started closed: {reason}")
        time.sleep(5)  # Blocking call
        logger.info(f"Spider closing: {reason}")
        self.complete_all()
        # You can call an async function here if needed
        logger.info(f"Spider completed closing: {reason}")
        # asyncio.run(self.final_tasks())


    def start_complete_all_main(self) :
        global logger
        logger.info(f'start_complete_all_main ...')
        self.complete_all()

    async def complete_all_async(self):
        # Extract the filename without the extension
        # await asyncio.sleep(0.1)  # Simulate a short delay
        self.complete_all()

    async def complete_all_main(self):
        complete_task=asyncio.create_task(self.complete_all_async())

    def parse(self, response):
        global logger
        global checkfile_path
        global bk_checkfile_path
        global chg_checkfile_path
        global last_url
        # global progress_full_urls

        logger.debug('parse ...')




        full_urls=[]
        for row in response.css('tr.row1, tr.row2'):
            # Extract all <td> elements in the row
            td_elements = row.css('td')
            full_url=None

            # Create a dictionary to hold the data
            data = {}
            dummy_data = {}

            # Iterate through each <td> and assign to dictionary
            for index, td in enumerate(td_elements):
                # Check if it's a link in the <td>
                link_url = td.css('a::attr(href)').get()
                if link_url and isinstance(link_url, str) :
                    full_url = urljoin(response.url, link_url)
                    # logger.debug(f'full_url : {full_url}')
                link = td.css('a.nolinelink::text').get()
                if link:
                    value = self.clean_text(link)  # Get the text of the link
                else:
                    # For other <td>, check if it contains HTML
                    value = self.clean_html_content(td.get())

                if index >= len(header_keys) :
                    key = f'TD_{index + 1}'  # Create a key like TD_1, TD_2, etc.
                else :
                    key=header_keys[index]
                data[key] = value
                dummy_data[key] = ''

            # Log the parsed data
            # location=data[header_keys[1]].strip()
            # logging.debug(f'Location : {location}.Parsed data: {data}')

            # Business
            # "note-issuing bank"
            # "Government T26 Contract"
            if 'HK' in data[header_keys[1]].strip() \
            and 'HK' in data['Order Location'].strip() \
            and 'Government T26 Contract' in data['Business'].strip() \
            and data['Job Title'] and (data['Job Title'].strip().startswith('Contract Systems Analyst') \
            or data['Job Title'].strip().startswith('Contract Senior Systems Analyst') \
            or data['Job Title'].strip().startswith('Contract Project Manager')) \
            :

                if full_url and isinstance(full_url, str) :
                    full_urls.append(full_url)
                    # yield Request(full_url, dont_filter=True)

        to_do_full_urls=list(set(full_urls))
        if to_do_full_urls and isinstance(to_do_full_urls, list) and len(to_do_full_urls) > 0 :

            # last_url=full_urls[-1]
            # logger.info(f'Last url is {full_urls[-1]}')

            # for i_full_url in full_urls :
            #     # yield from response.follow(i_full_url, callback=self.complete_all) 
            #     yield Request(i_full_url, dont_filter=True, callback=self.complete_all)
            yield from response.follow_all(to_do_full_urls, callback=self.parseDetails)

    def complete_all(self) :
        global checkfile_path
        global bk_checkfile_path
        global chg_checkfile_path
        global job_data_list
        global logger
        logger.debug(f'parse completed')
        logger.debug(f'parse bk_checkfile_path : {bk_checkfile_path}')
        global smtp_hostname
        global smtp_port

        # smtp_hostname='10.63.113.88'
        # smtp_port=25
        smtp_hostname='smtp.gmail.com'
        smtp_port=465



        if job_data_list :
            tmp_skills_list=[]
            self.all_skills_full_list.sort()
            [tmp_skills_list.extend(all_skills_full_list_itm) for all_skills_full_list_itm in self.all_skills_full_list]
            header_skills_list=list(set(tmp_skills_list))
            header_skills_list.sort()


            for job_data_list_itm in job_data_list :
                job_data_list_itm:dict
                jbo_skills_list=job_data_list_itm.keys()
                # job_data_list_itm.pop('skills')
                for header_skills_list_itm in header_skills_list :
                    job_data_list_itm.update({header_skills_list_itm : header_skills_list_itm if header_skills_list_itm in jbo_skills_list else ''})

            job_data_list_keys = list(job_data_list[0].keys())

            tmp_job_data_list_keys=list(job_data_list_keys)
            url_index = tmp_job_data_list_keys.index('URL')

            # Split the list into two parts
            before_url = tmp_job_data_list_keys[:url_index + 1]  # Include 'URL'
            after_url = tmp_job_data_list_keys[url_index + 1:]

            # Sort the second part
            after_url_sorted = sorted(after_url)

            # Combine both parts back
            sorted_job_data_list = before_url + after_url_sorted
            
            final_sorted_job_data_list=dict.fromkeys(sorted_job_data_list).keys()

            logger.debug(job_data_list[0].keys())
            logger.debug(sorted_job_data_list)
            logger.debug(final_sorted_job_data_list)
            logger.debug(tmp_job_data_list_keys)

            header_fieldname_list=final_sorted_job_data_list
            logger.debug(f'header_fieldname_list type is {type(header_fieldname_list)}')
            logger.debug(f'header_fieldname_list is {header_fieldname_list}')
            logger.debug(f'parse job_data_list[0].keys() : {header_fieldname_list}')
            logger.info(f'{job_data_list_itm.keys()}\n{final_sorted_job_data_list}')


            # Create an in-memory text stream
            output = io.StringIO()

            # Write the job data to the in-memory text stream
            writer = csv.DictWriter(output, fieldnames=header_fieldname_list)
            writer.writeheader()  # Write the header
            writer.writerows(job_data_list)  # Write multiple rows of data

            # Get the CSV content from the in-memory text stream
            csv_content = output.getvalue()

            # Save to a CSV file
            with open(checkfile_path, mode='w', newline='', encoding='utf-8') as file:
                file.write(csv_content)  # Write the CSV content to the file

            # Close the in-memory stream
            output.close()

            logger.debug(f"Job data has been written to {checkfile_path}")


            receipt_list=[
                'weikwan214@gmail.com'
                ,'ho.homer@gmail.com'
                # 'jkwei@try.gov.hk'
                # ,'hmho@try.gov.hk'
                # ,'ryhwong@try.gov.hk'
                          ]
            if os.path.isfile(bk_checkfile_path) \
                and os.path.lexists(bk_checkfile_path):
                    self.diff_two_csvs_files(bk_checkfile_path, checkfile_path, chg_checkfile_path)
                    # smtp_sender='cprsoperator@try.gov.hk'
                    if True :
                        smtp_sender='weikwan214@gmail.com'
                        self.send_email_with_restricted_marking_and_attachment(
                            smtp_sender,
                            '',
                            receipt_list,
                            [],
                            'Daily Operation Change',
                            '',
                            [chg_checkfile_path, checkfile_path]
                            , True, smtp_hostname, smtp_port
                        )


    def go_scrap(self, response, in_full_url) :
        yield from response.follow(in_full_url, callback=self.parseDetails)
        return True

        """
        Compare with previous cvs
        csv1 : previous version
        csv2 : current version
        """
    def diff_two_csvs_files(self, csv1, csv2, chg_checkfile_path) :
        global logger
        logger.info(f'Starting to diff both files : {csv1} vs {csv2} ...')
        # time.sleep(5)  # Blocking call

        logger.info(f'Begin to diff both files : {csv1} vs {csv2} ...')
        # time.sleep(5)  # Blocking call
        logger.debug(f'parse completed. chg_checkfile_path : {chg_checkfile_path}')


        tmp_load_csv_csv1=load_csv(open(csv1,encoding='utf-8-sig'))
        load_csv_csv1=tmp_load_csv_csv1.copy()
        
        tmp_load_csv_csv2=load_csv(open(csv2,encoding='utf-8-sig'))
        load_csv_csv2=tmp_load_csv_csv2.copy()


        # Read the entire CSV file into memory as a string
        with open(csv1, 'r', encoding='utf-8') as file:
            csv_content = file.read()

        excluded_column=['URL']
        with open(csv1, 'r', encoding='utf-8') as file:
            # Read the first line (header)
            header = file.readline().strip()

            
            # Split the header into columns
            columns = header.split(',')

            text = ' '.join(columns)

            # Define the regex pattern to match uppercase words with 3 to 5 characters
            pattern = r'\b[A-Z]{3,5}\b'

            # Find all matches
            extracted_abbreviations = re.findall(pattern, text)


        addon_opt_out_list=['Project Nature','Duties','Requirements']
        self.all_skills_full_list.append(addon_opt_out_list)


        # Extract abbreviations
        self.all_skills_full_list.append(list(set(extracted_abbreviations)))
        

        # Remove the 'JAV' column from each row
        tmp_columns_to_remove =[]
        [tmp_columns_to_remove.extend(all_skills_full_lst_itm) for all_skills_full_lst_itm in self.all_skills_full_list]
        columns_to_remove = list(set(tmp_columns_to_remove))
        columns_to_remove.sort()


        manual_keyfn = lambda r: hashlib.sha1(json.dumps(r, sort_keys=True).encode("utf8")).hexdigest()


        try:
            load_csv_csv1_keys=[]
            load_csv_csv1_keys.extend(load_csv_csv1.keys())
            for load_csv_csv1_key in load_csv_csv1_keys:
                tmp_load_csv_csv1_rowdata = load_csv_csv1.get(load_csv_csv1_key)
                load_csv_csv1_rowdata = tmp_load_csv_csv1_rowdata.copy()

                if isinstance(load_csv_csv1_rowdata, dict):  # Ensure it's a dictionary
                    for ctr_header in columns_to_remove:
                        load_csv_csv1_rowdata.pop(ctr_header, None)  # Remove specified headers safely

                    #recalculate the hash
                    recal_load_csv_csv1_key=manual_keyfn(load_csv_csv1_rowdata)
                    load_csv_csv1.pop(load_csv_csv1_key)

                    # Update the main dictionary
                    load_csv_csv1.update({recal_load_csv_csv1_key : tmp_load_csv_csv1_rowdata})
                else:
                    logger.error(f"Expected a dictionary for key {load_csv_csv1_key}, but got: {type(load_csv_csv1_rowdata)}")

        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(e)


        load_csv_csv2_keys=[]
        load_csv_csv2_keys.extend(load_csv_csv2.keys())
        for load_csv_csv2_key in load_csv_csv2_keys :
            tmp_load_csv_csv2_rowdata = load_csv_csv2.get(load_csv_csv2_key)
            load_csv_csv2_rowdata = tmp_load_csv_csv2_rowdata.copy()

            for ctr_header in columns_to_remove:
                    load_csv_csv2_rowdata.pop(ctr_header, None)
            #recalculate the hash
            recal_load_csv_csv2_key=manual_keyfn(load_csv_csv2_rowdata)
            load_csv_csv2.pop(load_csv_csv2_key)
            load_csv_csv2.update({recal_load_csv_csv2_key : tmp_load_csv_csv2_rowdata})




        logger.info(f'load_csv_csv2 : {type(load_csv_csv2)}')
        
        diff = compare(
            load_csv_csv1,
            load_csv_csv2,
            True
            )

        added_list=diff['added']
        removed_list=diff['removed']
        added_data=added_list
        removed_data=removed_list

        if added_data : keys_headers=added_data[0].keys()
        elif removed_data : keys_headers=removed_data[0].keys()

        output_buffer = io.StringIO()
        keys_headers=[]
        tmp_keys_headers=[]

        # log_info(f'added_data : {added_data}')
        if added_data : tmp_keys_headers.extend(added_data[0].keys())
        if removed_data : 
            if tmp_keys_headers :
                for key_in in removed_data[0].keys():
                    if key_in not in tmp_keys_headers :
                        tmp_keys_headers.append(key_in)
            else :
                tmp_keys_headers.extend(removed_data[0].keys())
        keys_headers.extend(tmp_keys_headers)
        if added_data or removed_data : pass
        else : keys_headers=['The DocInfo File is the same. No Chanage Found.']
        # log_info(f'added_data : {added_data}')

        keys_headers_list=[]
        if isinstance(keys_headers, dict) :
            keys_headers_list=list(keys_headers)
        elif isinstance(keys_headers, list) :
            keys_headers_list=keys_headers

        keys_headers_field='Job Key No'

        if keys_headers_list and isinstance(keys_headers_list, list) and len(keys_headers_list) > 0 :
            logger.debug(keys_headers_list[0])
            keys_headers_field=keys_headers_list[0]

        try :
            # Write to the buffer
            writer = csv.DictWriter(output_buffer, fieldnames=keys_headers, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()  # Write the header
            if keys_headers :
                if added_data and len(added_data) > 0 :
                    writer.writerows([{keys_headers_field: 'Added : '}])
                    writer.writerows(added_data)  # Write the data rows
                if removed_data and len(removed_data) > 0 :
                    writer.writerows([{keys_headers_field: 'Removed : '}])
                    writer.writerows(removed_data)  # Write the data rows
        except Exception as e:logger.error(traceback.format_exc());logger.error(e);logger.error(f'removed_data : {removed_data}')

        # Get the content of the buffer
        csv_content = output_buffer.getvalue()

        # Specify the output CSV file path
        with open(chg_checkfile_path, 'w', encoding='utf-8-sig', newline='') as file:
            file.write(csv_content)
        logger.info(f'Completed diff both files : {csv1} vs {csv2} ...')



    def clean_html_content_duties(self, html_content):
        # print(f'html_content : {html_content}')

        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract text and replace <br> tags with spaces
        text = soup.get_text(separator=' ', strip=True)
        if text : text.replace('\n', ' ')
        # self.logger.info(f'html_content text : {text}|\ntext : {html_content}')
        # self.logger.info(f'html_content text ***************** : {len(text)}')
        return text
