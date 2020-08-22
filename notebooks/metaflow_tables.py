from metaflow import FlowSpec,step

class TableAnalysisFlow(FlowSpec):
    
    @step
    def start(self):
        from pathlib import Path
        import random
        import os
        self.ROOT_PATH = Path('data')
        source_path = self.ROOT_PATH / 'sources'
        # self.arxiv_paths = random.sample(os.listdir(source_path),2)
        self.arxiv_paths = os.listdir(source_path) #random.sample(,2)
        self.next(self.table_extractor,foreach='arxiv_paths')
    
    @step
    def table_extractor(self):
        from axcell.helpers.paper_extractor import PaperExtractor
        from axcell.data.paper_collection import PaperCollection
        extract = PaperExtractor(self.ROOT_PATH)
        SOURCE_PATH = self.ROOT_PATH / 'sources' / self.input
        try:
            extract(SOURCE_PATH)
            self.paper_id = self.input.replace('.tar.gz','')
            PAPERS_PATH = self.ROOT_PATH / 'papers'
            pc = PaperCollection.from_files(PAPERS_PATH)
            self.paper = pc.get_by_id(self.paper_id)    
        except:
            self.paper=None
            self.paper_id=None
            
        self.next(self.join)
    
    @step
    def join(self,inputs):
        self.processed_papers = []
        for i in inputs:
            self.processed_papers.append((i.paper_id,i.paper))
        self.next(self.end) 
    
    @step
    def end(self):
        print("Done Computation")



if __name__ == '__main__':
    TableAnalysisFlow()



    