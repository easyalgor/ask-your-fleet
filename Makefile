.PHONY: help setup check run test generate-data clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Install Python dependencies
	pip install -r requirements.txt

check: ## Verify Confluent Cloud connectivity
	python tests/test_connection.py

run: ## Start the MCP server (stdio mode)
	python main.py

test: ## Run the integration test suite against live Flink
	python tests/test_flink_tools.py

generate-data: ## Start the MQTT sensor data generator (requires mosquitto)
	@if [ ! -f .env.mqtt ]; then \
		echo "❌  Copy .env.mqtt.example → .env.mqtt and fill in your MQTT broker credentials first."; \
		exit 1; \
	fi
	@source .env.mqtt && bash sensor_generator.sh

clean: ## Remove Python cache files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true
