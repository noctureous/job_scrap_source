# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class JobScrapPipeline:
    def process_item(self, item, spider):
        print(f'process_item')
        return item

    def close_spider(self, spider):
        # This method is called when the spider is closed
        print(f"Post-processing tasks after {spider.name} has finished.")
        
        # Run any final tasks
        self.run_final_tasks()

    def run_final_tasks(self):
        # Implement your post-processing logic here
        print("Running final tasks...")
        # Example: Perform cleanup, save results, etc.