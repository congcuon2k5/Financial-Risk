package com.creditscore.controller;

import com.creditscore.model.ScoreResponse;
import com.creditscore.service.ScoreService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class ScoreController {

    private final ScoreService scoreService;

    public ScoreController(ScoreService scoreService) {
        this.scoreService = scoreService;
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "OK");
    }

    @GetMapping("/score/{skIdCurr}")
    public ResponseEntity<ScoreResponse> getScore(@PathVariable long skIdCurr) {
        return scoreService.getScore(skIdCurr)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
