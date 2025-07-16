from mrjob.job import MRJob
from mrjob.step import MRStep

class TopN(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_count),
            MRStep(reducer=self.reducer_max)
        ]

    def mapper(self, _, line):
        data = line.strip().split(',')

  
        if 'status_type' in line.lower():
            return

        if len(data) > 2:
            try:
                status_type = data[1].strip().lower()
                date_str = data[2].strip()

              
                if ' ' in date_str:
                    year = date_str.split(' ')[0].split('/')[2]
                    if status_type in ['photo', 'status', 'video', 'link']:
                        yield (year, status_type), 1
            except IndexError:
                pass

    def reducer_count(self, key, values):
        year, status_type = key
        count = sum(values)
        yield year, (count, status_type)

    def reducer_max(self, year, values):
   
        top2 = sorted(values, reverse=True, key=lambda x: x[0])[:2]

        # ทำให้ status type แสดงด้วยตัวใหญ่
        top2_capitalized = [[count, status.title()] for count, status in top2]
        yield year, top2_capitalized

if __name__ == '__main__':
    TopN.run()
