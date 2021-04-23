package com.redpillanalytics.controllers

import com.redpillanalytics.utils.Gcp
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
class PostSessions {
   @Post(value = "/", consumes = MULTIPART_FORM_DATA, produces = TEXT_PLAIN)
   Single<HttpResponse<String>> upload(StreamingFileUpload file) {

      File tempFile = File.createTempFile(file.filename,'')
      Publisher<Boolean> uploadPublisher = file.transferTo(tempFile)
      log.info "File $tempFile created."

      String contextId = UUID.randomUUID().toString()

      Gcp gcp = new Gcp(dataset: contextId)

      Single.fromPublisher(uploadPublisher)
              .map({ success ->
                 if (success) {
                    log.info "Size: ${tempFile.size()}"
                    gcp.uploadFile(tempFile)
                    HttpResponse.ok("Sessions file posted.")
                 } else {
                    HttpResponse.<String> status(CONFLICT)
                            .body("Sessions post failed.")
                 }
              })
   }
}