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
        reader = csv.reader(elem.splitlines(), delimiter=',')
        row = next(reader)

        return {
                "PassengerId": row[0],
                "Survived": row[1],
                "Pclass": row[2],
                "Name": row[3],
                "Sex": row[4],
                "Age": row[5],
                "SibSp": row[6],
                "Parch" : row[7],
                "Ticket": row[8],
                "Fare": row[9],
                "Cabin": row[10],
                "Embarked": row[11],
                }

    def sexMap(elem):
        sex_map = {"female": 0, "male": 1}
        elem["Sex"] = sex_map[elem["Sex"]]

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


if __name__ == "__main__":
    run()
