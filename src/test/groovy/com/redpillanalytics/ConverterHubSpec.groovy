package com.redpillanalytics

import io.micronaut.runtime.EmbeddedApplication
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification
import javax.inject.Inject

@MicronautTest
class ConverterHubSpec extends Specification {

    @Inject
    EmbeddedApplication<?> application

    def 'test it works'() {
        expect:
        application.running
    }

}
