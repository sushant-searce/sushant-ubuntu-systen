from apache_beam.options.pipeline_options import GoogleCloudOptions
import apache_beam as beam

pipeline_options = GoogleCloudOptions(flags=sys.argv[1:])
pipeline_options.project = gce-249612
pipeline_options.region = 'us-central1'
pipeline_options.job_name = JOB_DATAFLOW    
pipeline_options.staging_location = BUCKET + 'gs://test_of_dataflow/people.csv/binaries'
pipeline_options.temp_location = BUCKET + 'gs://test_of_dataflow/temp'

schema = 'Name:STRING,Age:STRING,Job:STRING,Marks:STRING,Aggr:STRING'
p = (beam.Pipeline(options = pipeline_options)
     | 'ReadFromGCS' >> beam.io.textio.ReadFromText('gs://test_of_dataflow/people.csv)
     | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('gce-249612:qwerty.people', schema = schema))