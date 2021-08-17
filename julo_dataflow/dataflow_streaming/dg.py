import os
import apache_beam as beam
import logging
from apache_beam import window
from google.cloud import storage
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/sushantnigudkar/Documents/key.json"
PROJECT = 'searce-playground'
BUCKET = 'sushant-julo-test-blucket'
schema = 'Name:STRING,Age:STRING,Job:STRING,Marks:STRING,Aggr:STRING'

# class Rename(beam.DoFn):
#    def process(self, element):
# #       file1="result.csv"
#        storage_client = storage.Client()
#        bucket = storage_client.get_bucket("sushant-julo-test-blucket")
#        blob = bucket.blob(element)
#        new_blob = bucket.rename_blob(blob, "New/"+element)
#        print('Blob {} has been renamed to {}'.format(blob.name, new_blob.name))

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
           '--staging_location=gs://sushant-julo-test-blucket/staging/',
           '--temp_location=gs://sushant-julo-test-blucket/tmp/',
           '--runner=Dataflow'
          ]
   p = beam.Pipeline(argv=argv)
   (p
       | 'ReadFromText' >> beam.io.ReadFromText('gs://sushant-julo-test-blucket/people5.csv')
       | 'ParseCSV' >> beam.ParDo(Split())
       | 'window' >> beam.WindowInto(window.Sessions(60))
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('searce-playground:sushant_julo.people', schema=schema,
       write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    #    | 'rename file' >> beam.ParDo(Rename())
   )
   p.run()


if __name__ == '__main__':
   logging.getLogger().setLevel(logging.INFO)
   run()