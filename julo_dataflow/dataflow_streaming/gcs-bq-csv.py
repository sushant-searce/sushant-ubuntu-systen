import apache_beam as beam
import os
import logging
from apache_beam import window
# from google.cloud import storage


PROJECT = 'searce-playground'
BUCKET = 'test_of_dataflow'
schema = 'Name:STRING,Age:STRING,Job:STRING,Marks:STRING,Aggr:STRING'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/sushantnigudkar/Documents/key.json"


class Split(beam.DoFn):

    def process(self, element):
        Name, Age, Job, Marks, Aggr = element.split(",")

        return [{
            'Name': Name,
            'Age': Age,
            'Job': Job,
            'Marks': Marks,
            'Aggr': Aggr,
        }]


def run():

    argv = [
        '--project={0}'.format(PROJECT),
        '--staging_location=gs://sushant-julo-test-blucket/staging/'.format(BUCKET),
        '--temp_location=gs://sushant-julo-test-blucket/tmp/'.format(BUCKET),
        '--runner=Dataflow'
    ]

    p = beam.Pipeline(argv=argv)

    (p
        | 'ReadFromText' >> beam.io.ReadFromText('gs://sushant-julo-test-blucket/peo*.csv')
        | 'ParseCSV' >> beam.ParDo(Split())
        | 'window' >> beam.WindowInto(window.Sessions(60))
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('searce-playground:sushant_julo.people'.format(PROJECT), schema=schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        # | 'rename file' >> beam.ParDo(rename())

    )

    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
