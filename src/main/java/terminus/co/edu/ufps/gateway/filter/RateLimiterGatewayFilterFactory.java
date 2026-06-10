package terminus.co.edu.ufps.gateway.filter;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Filtro de Rate Limiting por IP usando Bucket4j (en memoria, sin Redis).
 */
@Component
public class RateLimiterGatewayFilterFactory extends AbstractGatewayFilterFactory<RateLimiterGatewayFilterFactory.Config> {

    // Un bucket por IP. ConcurrentHashMap es thread-safe para el entorno reactivo.
    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    public RateLimiterGatewayFilterFactory() {
        super(Config.class);
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> {
            String ip = exchange.getRequest().getRemoteAddress() != null
                    ? exchange.getRequest().getRemoteAddress().getAddress().getHostAddress()
                    : "unknown";

            Bucket bucket = buckets.computeIfAbsent(ip, k -> createBucket(config));
            long remainingTokens = bucket.getAvailableTokens();

            if (bucket.tryConsume(1)) {
                // Agrega headers informativos al response
                exchange.getResponse().getHeaders()
                        .add("X-RateLimit-Remaining", String.valueOf(remainingTokens - 1));
                return chain.filter(exchange);
            }

            // 429 Too Many Requests
            exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
            exchange.getResponse().getHeaders()
                    .add("X-RateLimit-Remaining", "0");
            exchange.getResponse().getHeaders()
                    .add("Retry-After", String.valueOf(config.getRefillDurationSeconds()));
            return exchange.getResponse().setComplete();
        };
    }

    private Bucket createBucket(Config config) {
        // Token Bucket: capacidad máxima + recarga gradual
        Refill refill = Refill.greedy(config.getRefillTokens(),
                Duration.ofSeconds(config.getRefillDurationSeconds()));
        Bandwidth limit = Bandwidth.classic(config.getCapacity(), refill);
        return Bucket.builder().addLimit(limit).build();
    }

    public static class Config {
        // Tokens máximos que puede acumular el bucket (burst máximo)
        private int capacity = 60;
        // Tokens que se recargan cada 'refillDurationSeconds'
        private int refillTokens = 60;
        // Cada cuántos segundos se recargan los tokens
        private int refillDurationSeconds = 60;

        public int getCapacity() { return capacity; }
        public void setCapacity(int capacity) { this.capacity = capacity; }
        public int getRefillTokens() { return refillTokens; }
        public void setRefillTokens(int refillTokens) { this.refillTokens = refillTokens; }
        public int getRefillDurationSeconds() { return refillDurationSeconds; }
        public void setRefillDurationSeconds(int refillDurationSeconds) {
            this.refillDurationSeconds = refillDurationSeconds;
        }
    }
}
