package com.coelhoworks.coelhosensors.temperature.processing.api.controller;

import com.coelhoworks.coelhosensors.temperature.processing.api.model.TemperatureLogOutput;
import com.coelhoworks.coelhosensors.temperature.processing.common.IdGenerator;
import io.hypersistence.tsid.TSID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.OffsetDateTime;

import static com.coelhoworks.coelhosensors.temperature.processing.infrastructure.rabbitmq.RabbitMQConfig.FANOUT_EXCHANGE;

@RestController
@RequestMapping("/api/sensors/{sensorId}/temperatures/data")
@Slf4j
@RequiredArgsConstructor
public class TemperatureProcessingController {

  private final RabbitTemplate rabbitTemplate;

  @PostMapping(consumes = MediaType.TEXT_PLAIN_VALUE)
  public void data(@PathVariable TSID sensorId, @RequestBody String input){

    if (input == null || input.isBlank()){
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
    }

    Double temperature;

    try {
      temperature = Double.parseDouble(input);
    } catch (NumberFormatException e){
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
    }

    TemperatureLogOutput logOutput = TemperatureLogOutput.builder()
            .id(IdGenerator.generateTimeBasedUUID())
            .sensorId(sensorId)
            .value(temperature)
            .registeredAt(OffsetDateTime.now())
            .build();

    log.info(logOutput.toString());

    String exchange = FANOUT_EXCHANGE;
    String routingKey = "";

    String payload = logOutput.toString();

    rabbitTemplate.convertAndSend(exchange, routingKey, payload);

  }
}
