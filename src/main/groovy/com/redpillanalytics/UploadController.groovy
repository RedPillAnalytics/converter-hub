package com.redpillanalytics

import groovy.util.logging.Slf4j
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.multipart.StreamingFileUpload
import io.reactivex.Single
import org.reactivestreams.Publisher

import static io.micronaut.http.HttpStatus.CONFLICT
import static io.micronaut.http.MediaType.MULTIPART_FORM_DATA
import static io.micronaut.http.MediaType.TEXT_PLAIN

@Slf4j
@Controller("/sessions")
class UploadController {

   @Post(value = "/", consumes = MULTIPART_FORM_DATA, produces = TEXT_PLAIN)
   Single<HttpResponse<String>> upload(StreamingFileUpload file) {

      File tempFile = File.createTempFile(file.filename, "temp")
      Publisher<Boolean> uploadPublisher = file.transferTo(tempFile)
      log.info "File $file uploaded."

      Single.fromPublisher(uploadPublisher)
              .map({ success ->
                 if (success) {
                    HttpResponse.ok("Uploaded")
                 } else {
                    HttpResponse.<String> status(CONFLICT)
                            .body("Upload Failed")
                 }
              })
   }
}