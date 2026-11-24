package com.sau;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
public class EmployeeController {

    private static final int MAX_DETAIL = 3;

    private final JdbcTemplate jdbc;
    private final SparkService spark;

    public EmployeeController(JdbcTemplate jdbc, SparkService spark) {
        this.jdbc = jdbc;
        this.spark = spark;
    }

    @GetMapping("/")
    public String index(@RequestParam(required = false) String name, Model model) {
        String sql = "SELECT e.empno, e.ename, m.ename AS mgr_name, e.sal, e.comm, d.dname, e.img "
                + "FROM emp e "
                + "LEFT JOIN emp m ON e.mgr = m.empno "
                + "LEFT JOIN dept d ON e.deptno = d.deptno ";

        boolean search = name != null && !name.trim().isEmpty();
        List<Map<String, Object>> employees = search
                ? jdbc.queryForList(sql + "WHERE UPPER(e.ename) LIKE ? ORDER BY e.empno",
                "%" + name.trim().toUpperCase() + "%")
                : jdbc.queryForList(sql + "ORDER BY e.empno");

        Map<Integer, String> totals;
        Map<Integer, List<Map<String, Object>>> records = new HashMap<>();
        if (search) {
            // sadece bulunan kullanicilar icin Spark'ta filtrele
            totals = new HashMap<>();
            boolean detail = employees.size() <= MAX_DETAIL;
            for (Map<String, Object> e : employees) {
                int empno = ((Number) e.get("empno")).intValue();
                totals.put(empno, spark.total(empno));
                if (detail) {
                    records.put(empno, spark.records(empno));
                }
            }
        } else {
            totals = spark.totals();
        }

        model.addAttribute("employees", employees);
        model.addAttribute("totals", totals);
        model.addAttribute("records", records);
        model.addAttribute("name", search ? name.trim() : "");
        return "index";
    }

    // Tek kullanici sorgusu: /total/7788
    @GetMapping("/total/{empno}")
    @ResponseBody
    public String total(@PathVariable int empno) {
        return spark.total(empno);
    }
}