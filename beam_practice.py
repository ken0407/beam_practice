import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

class MyOption(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--output", dest="output")

def run():
    def csvParse(elem):
        import csv
        text = elem.encode('utf-8')
        reader = csv.reader(text.splitlines(), delimiter=',')
        row = next(reader)
        print(row)

        return {
                "PassengerId": int(row[0]),
                "Survived": int(row[1]),
                "Pclass": int(row[2]),
                "Name": str(row[3]),
                "Sex": str(row[4]),
                "Age": int(row[5]),
                "SibSp": int(row[6]),
                "Parch" : int(row[7]),
                "Ticket": str(row[8]),
                "Fare": float(row[9]),
                "Cabin": str(row[10]),
                "Embarked": str(row[11]),
                }

    def sexMap(elm):
        sex_map = {"female":0, "male": 1}
        elem["Sex"] = sex_map(elem["Sex"])

        return elem

    options = MyOption()
    with beam.Pipeline(options=options) as p:
        _ = (
                p
                | ReadFromText("data/titanic.csv", skip_header_lines=1)
                | beam.Map(csvParse)
                | beam.Map(sexMap)
                | WriteToText(f"data/{options.output}", file_name_suffix=".txt")
            )

if __name__=="__main__":
    run()
