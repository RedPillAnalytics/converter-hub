package com.redpillanalytics.utils

import com.google.cloud.RetryOption
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.CsvOptions
import com.google.cloud.bigquery.FormatOptions
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.LoadJobConfiguration
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.Table
import com.google.cloud.bigquery.TableId
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.StorageOptions
import groovy.util.logging.Slf4j
import org.threeten.bp.Duration

import static java.nio.charset.StandardCharsets.UTF_8

@Slf4j
class Gcp {
   /**
    * Project ID Constant
    */
   static String PROJECTID = 'rpa-converter'

   /**
    * The Google Cloud Storage bucket to use.
    */
   static String BUCKET = 'rpa-converter'

   /**
    * The BigQuery dataset name to use.
    */
   String dataset

   /**
    * BigQuery client used to send requests.
    */
   def getBigQuery() {
      return BigQueryOptions
              .getDefaultInstance()
              .toBuilder()
              .setProjectId(PROJECTID)
              .build()
              .getService()
   }

   /**
    * Google Cloud storage client used to upload files.
    */
   def getStorage() {
      return StorageOptions.getDefaultInstance().getService()
   }

   /**
    * Upload file to a GCS bucket.
    */
   def uploadFile(File file) {
      try {
         BlobId blobId = BlobId.of(BUCKET, "${dataset}/${file.name}")
         BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build()
         storage.create(blobInfo, file.text.getBytes(UTF_8))
      } catch (Exception e) {
         throw e
      }
   }

   /**
    * Load a BigQuery table with a CSV file
    */
   def loadTable(String table, String key) {
      //object file key
      //String key = "${file.parentFile.name}/${file.name}"

      TableId tableId = TableId.of(PROJECTID, dataset, table)
      log.debug "TableId: ${tableId}"

      LoadJobConfiguration configuration =
              LoadJobConfiguration.newBuilder(tableId, "gs://${BUCKET}/${dataset}/${key}")
                      .setAutodetect(true)
                      .setFormatOptions(CsvOptions.newBuilder().setAllowQuotedNewLines(true).build())
                      .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                      .build()

      Job job = bigQuery.create(JobInfo.of(configuration))
      job = job.waitFor()
      return job.toString()
   }

   /**
    * Create a table from a select statement.
    */
   def createTableAsSelect(String query, String tableName) {
      try {

         // Identify the destination table
         TableId tableId = TableId.of(PROJECTID, dataset, tableName)

         // Build the query job
         QueryJobConfiguration queryConfig =
                 QueryJobConfiguration
                         .newBuilder(query)
                         .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                         .setDestinationTable(tableId)
                         .build()

         // Execute the query.
         bigQuery.query(queryConfig)

      } catch (BigQueryException | InterruptedException e) {
         log.warn "Saved query did not run:\n$e"
         throw e
      }
   }

   def distinctTable(String source, String target, String datasetName = this.dataset) {
      createTableAsSelect("select distinct * from ${datasetName}.${source}", target)
   }

   def exportTable(String tableName, String path) {
      try {
         TableId tableId = TableId.of(PROJECTID, dataset, tableName)
         Table table = bigQuery.getTable(tableId)

         Job job = table.extract(new FormatOptions().json().getType(), "gs://${BUCKET}/${path}")

         // Blocks until this job completes its execution, either failing or succeeding.
         Job completedJob =
                 job.waitFor(
                         RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
                         RetryOption.totalTimeout(Duration.ofMinutes(3)))
         if (completedJob == null) {
            log.warn "Job not executed since it no longer exists."
            return
         } else if (completedJob.getStatus().getError() != null) {
            log.warn "BigQuery was unable to extract due to an error: \n${job.getStatus().getError()}"
            return
         }
         log.info "Table export successful."
      } catch (BigQueryException | InterruptedException e) {
         log.warn "Table extraction job was interrupted."
         throw e
      }
   }
}
