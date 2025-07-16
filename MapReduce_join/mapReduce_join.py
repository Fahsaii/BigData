from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from io import StringIO

class JoinPostsByStatusID(MRJob):

    def configure_args(self):
        super(JoinPostsByStatusID, self).configure_args()
        self.add_passthru_arg(
            '--join-type',
            default='inner',
            choices=['inner', 'left_outer', 'right_outer', 'full_outer'],
            help='Type of join to perform (inner, left_outer, right_outer, full_outer)'
        )
        self.add_passthru_arg(
            '--output-file',
            default='join_output.txt',
            help='Output file name for the table results'
        )

    def format_output_row(self, row):
        return f"{row['record_id']:<25} | {row['type']:<8} | {row['published']:<18} | {str(row['reactions_left']):<15} | {str(row['reactions_right']):<15} | {str(row['comments']):<12} | {str(row['shares']):<10}"

    def mapper(self, _, line):
        if line.strip().startswith('Table,status_id'):
            return
        
        reader = csv.reader(StringIO(line))
        columns = next(reader)

        if len(columns) >= 6:
            table_name, record_id, post_type, publish_time, reactions = columns[:5]

            if len(columns) == 6 and table_name == 'FB2':
                comments = columns[5]
                yield record_id, ('left', {
                    'type': post_type,
                    'published': publish_time,
                    'reactions': reactions,
                    'comments': comments
                })
            elif len(columns) == 6 and table_name == 'FB3':
                shares = columns[5]
                yield record_id, ('right', {
                    'type': post_type,
                    'published': publish_time,
                    'reactions': reactions,
                    'shares': shares
                })

    def reducer(self, record_id, entries):
        left_table_data = []
        right_table_data = []

        for source, info in entries:
            if source == 'left':
                left_table_data.append(info)
            elif source == 'right':
                right_table_data.append(info)

        join_mode = self.options.join_type

        if join_mode == 'inner':
            if left_table_data and right_table_data:
                for left in left_table_data:
                    for right in right_table_data:
                        result = {
                            'record_id': record_id,
                            'type': left['type'],
                            'published': left['published'],
                            'reactions_left': left['reactions'],
                            'reactions_right': right['reactions'],
                            'comments': left['comments'],
                            'shares': right['shares']
                        }
                        yield record_id, self.format_output_row(result)

        elif join_mode == 'left_outer':
            if left_table_data:
                if right_table_data:
                    for left in left_table_data:
                        for right in right_table_data:
                            result = {
                                'record_id': record_id,
                                'type': left['type'],
                                'published': left['published'],
                                'reactions_left': left['reactions'],
                                'reactions_right': right['reactions'],
                                'comments': left['comments'],
                                'shares': right['shares']
                            }
                            yield record_id, self.format_output_row(result)
                else:
                    for left in left_table_data:
                        result = {
                            'record_id': record_id,
                            'type': left['type'],
                            'published': left['published'],
                            'reactions_left': left['reactions'],
                            'reactions_right': None,
                            'comments': left['comments'],
                            'shares': None
                        }
                        yield record_id, self.format_output_row(result)

        elif join_mode == 'right_outer':
            if right_table_data:
                if left_table_data:
                    for left in left_table_data:
                        for right in right_table_data:
                            result = {
                                'record_id': record_id,
                                'type': left['type'],
                                'published': left['published'],
                                'reactions_left': left['reactions'],
                                'reactions_right': right['reactions'],
                                'comments': left['comments'],
                                'shares': right['shares']
                            }
                            yield record_id, self.format_output_row(result)
                else:
                    for right in right_table_data:
                        result = {
                            'record_id': record_id,
                            'type': right['type'],
                            'published': right['published'],
                            'reactions_left': None,
                            'reactions_right': right['reactions'],
                            'comments': None,
                            'shares': right['shares']
                        }
                        yield record_id, self.format_output_row(result)

        elif join_mode == 'full_outer':
            if left_table_data and right_table_data:
                for left in left_table_data:
                    for right in right_table_data:
                        result = {
                            'record_id': record_id,
                            'type': left['type'],
                            'published': left['published'],
                            'reactions_left': left['reactions'],
                            'reactions_right': right['reactions'],
                            'comments': left['comments'],
                            'shares': right['shares']
                        }
                        yield record_id, self.format_output_row(result)
            elif left_table_data:
                for left in left_table_data:
                    result = {
                        'record_id': record_id,
                        'type': left['type'],
                        'published': left['published'],
                        'reactions_left': left['reactions'],
                        'reactions_right': None,
                        'comments': left['comments'],
                        'shares': None
                    }
                    yield record_id, self.format_output_row(result)
            elif right_table_data:
                for right in right_table_data:
                    result = {
                        'record_id': record_id,
                        'type': right['type'],
                        'published': right['published'],
                        'reactions_left': None,
                        'reactions_right': right['reactions'],
                        'comments': None,
                        'shares': right['shares']
                    }
                    yield record_id, self.format_output_row(result)

if __name__ == '__main__':
    JoinPostsByStatusID.run()
