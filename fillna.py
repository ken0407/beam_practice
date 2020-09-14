import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


class FillnaOption(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--output', default='fillna')


def csvParser(elem):
    import csv
    reader = csv.reader(elem.splitlines(), delimiter=',')
    row = next(reader)

    return {"PassengerId": row[0],
            "Age": row[5],
            }


class Fillna(beam.DoFn):

    def __init__(self):
        pass

    def process(self, elem):
        if not elem["Age"]:
            elem["Age"] = 0
        elem["Age"] = float(elem["Age"])
        yield elem


def run():
    options = FillnaOption()
    with beam.Pipeline(options=options) as p:
        _ = (
                p
                | ReadFromText("data/titanic.csv", skip_header_lines=1)
                | beam.Map(csvParser)
                | beam.ParDo(Fillna())
                | WriteToText(f"data/{options.output}", file_name_suffix='.txt')
        )


if __name__ == "__main__":
    run()
