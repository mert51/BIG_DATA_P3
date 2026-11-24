package com.sau;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.net.URI;
import java.util.List;
import java.util.Locale;

@Controller
public class UploadController {

    private static final String HDFS_URI = "hdfs://localhost:9000";

    private final JdbcTemplate jdbc;

    public UploadController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @PostMapping("/upload")
    public String upload(@RequestParam int empno,
                         @RequestParam("file") MultipartFile file,
                         RedirectAttributes ra) {
        String original = file.getOriginalFilename() == null
                ? "" : file.getOriginalFilename().toLowerCase(Locale.ROOT);

        if (file.isEmpty()) {
            ra.addFlashAttribute("message", "Please choose a file.");
            return "redirect:/";
        }
        if (!(original.endsWith(".jpg") || original.endsWith(".jpeg"))) {
            ra.addFlashAttribute("message", "Only .jpg files are accepted.");
            return "redirect:/";
        }

        List<String> imgs = jdbc.queryForList("SELECT img FROM emp WHERE empno = ?", String.class, empno);
        if (imgs.isEmpty()) {
            ra.addFlashAttribute("message", "Employee not found.");
            return "redirect:/";
        }
        String img = imgs.get(0);

        try (FileSystem fs = FileSystem.newInstance(URI.create(HDFS_URI), new Configuration());
             FSDataOutputStream out = fs.create(new Path("/images/" + img), true)) {
            out.write(file.getBytes());
            ra.addFlashAttribute("message", "Saved to HDFS: /images/" + img);
        } catch (Exception e) {
            ra.addFlashAttribute("message", "HDFS error: " + e.getMessage());
        }
        return "redirect:/";
    }
}