package com.example.demo;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Value;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

@SpringBootApplication
public class QualifierJavaApplication implements CommandLineRunner {

    @Value("${bfh.name}") private String name;
    @Value("${bfh.regNo}") private String regNo;
    @Value("${bfh.email}") private String email;

    private final RestTemplate rest = new RestTemplate();

    public static void main(String[] args) {
        SpringApplication.run(QualifierJavaApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        String genUrl = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";
        Map<String, String> genBody = Map.of("name", name, "regNo", regNo, "email", email);
        HttpHeaders genHdr = new HttpHeaders();
        genHdr.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<Map> genResp = rest.postForEntity(genUrl, new HttpEntity<>(genBody, genHdr), Map.class);

        if (!genResp.getStatusCode().is2xxSuccessful() || genResp.getBody() == null) {
            throw new IllegalStateException("GenerateWebhook failed: " + genResp.getStatusCode());
        }
        String webhook = String.valueOf(genResp.getBody().get("webhook"));
        String accessToken = String.valueOf(genResp.getBody().get("accessToken"));

        String finalQuery =
                "SELECT \n" +
                "    e1.EMP_ID,\n" +
                "    e1.FIRST_NAME,\n" +
                "    e1.LAST_NAME,\n" +
                "    d.DEPARTMENT_NAME,\n" +
                "    COUNT(e2.EMP_ID) AS YOUNGER_EMPLOYEES_COUNT\n" +
                "FROM EMPLOYEE e1\n" +
                "JOIN DEPARTMENT d ON e1.DEPARTMENT = d.DEPARTMENT_ID\n" +
                "LEFT JOIN EMPLOYEE e2 \n" +
                "  ON e1.DEPARTMENT = e2.DEPARTMENT\n" +
                " AND e2.DOB > e1.DOB\n" +
                "GROUP BY e1.EMP_ID, e1.FIRST_NAME, e1.LAST_NAME, d.DEPARTMENT_NAME\n" +
                "ORDER BY e1.EMP_ID DESC;";

        Files.writeString(Path.of("final-query.sql"), finalQuery, StandardCharsets.UTF_8);

        Map<String, String> payload = Map.of("finalQuery", finalQuery);
        HttpHeaders subHdr = new HttpHeaders();
        subHdr.setContentType(MediaType.APPLICATION_JSON);
        subHdr.set("Authorization", accessToken);

        ResponseEntity<String> submitResp;
        try {
            submitResp = rest.postForEntity(webhook, new HttpEntity<>(payload, subHdr), String.class);
        } catch (HttpClientErrorException.Unauthorized e) {
            subHdr = new HttpHeaders();
            subHdr.setContentType(MediaType.APPLICATION_JSON);
            subHdr.setBearerAuth(accessToken);
            submitResp = rest.postForEntity(webhook, new HttpEntity<>(payload, subHdr), String.class);
        }

        System.out.println("Submit status: " + submitResp.getStatusCode());
        System.out.println("Submit body: " + submitResp.getBody());
    }
}
