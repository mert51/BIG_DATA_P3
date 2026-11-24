package com.sau;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

@RestController
public class ImageController {

    private static final String HDFS_URI = "hdfs://localhost:9000";

    @GetMapping("/img/{name}")
    public ResponseEntity<byte[]> image(@PathVariable String name) throws IOException {
        // sadece "scott.jpg" gibi duz dosya adlarina izin ver
        if (!name.matches("[A-Za-z0-9_-]+\\.jpg")) {
            return ResponseEntity.badRequest().build();
        }

        Configuration conf = new Configuration();
        try (FileSystem fs = FileSystem.newInstance(URI.create(HDFS_URI), conf);
             FSDataInputStream in = fs.open(new Path("/images/" + name))) {
            byte[] data = in.readAllBytes();
            return ResponseEntity.ok().contentType(MediaType.IMAGE_JPEG).body(data);
        } catch (FileNotFoundException e) {
            return ResponseEntity.notFound().build();
        }
    }
}