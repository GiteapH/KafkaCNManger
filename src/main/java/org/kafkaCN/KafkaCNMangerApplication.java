package org.kafkaCN;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@EnableSwagger2
@SpringBootApplication
@MapperScan("org.kafkaCN.mapper")
public class KafkaCNMangerApplication {

    public static int tempTag=0;
    public static void main(String[] args) {
        SpringApplication.run(KafkaCNMangerApplication.class, args);
    }
}
