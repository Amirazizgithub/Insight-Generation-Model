# Insight Generation Microservice

AI-powered marketing insights generator for multi-source data analysis including Google Ads, Google Analytics, Meta Ads, Databases, and more.

## 🚀 Features

- **CDP Insights**: Generate actionable insights from Customer Data Platform data
- **Intelligence Dashboard**: AI-driven analysis for marketing intelligence
- **Health Monitoring**: Built-in health check endpoints
- **FastAPI Framework**: High-performance async web framework
- **Google Vertex AI Integration**: Leverages Gemini models for insight generation
- **Docker Support**: Containerized deployment
- **GitLab CI/CD**: Automated pipelines for development, staging, and production

## 📂 Project Structure

```
insight-generation-model/
├── app/
│   ├── __init__.py                  # FastAPI application entry point
│   ├── models/
│   │   ├── __init__.py              # Models package
│   │   ├── data_analysis.py         # Build data analysis report for insights generations
│   │   ├── data_laoder.py           # Query selection and data loading from big query
│   │   └── insight_model.py         # Insight generation service using Vertex AI
│   ├── api/
│   │   ├── __init__.py              # API package
│   │   ├── health_router.py         # Health check endpoints
│   │   ├── cdp_insight_router.py    # CDP Insight API endpoints
│   │   └── intelligence_insight_router.py   # Intelligence API endpoints
│   ├── prompts/
│   │   ├── __init__.py              # Prompts package
│   │   └── prompt.py                # AI prompts for insight generation
│   └── utils/
│       ├── __init__.py              # Utils package
│       ├── logger.py                # Logging configuration
│       └── main_utils.py            # Utility functions for data processing
├── gitlab-pipelines/
│   ├── .gitlab-ci-dev.yml           # Development pipeline (formatting, linting)
│   ├── .gitlab-ci-prod.yml          # Production deployment pipeline
│   └── .gitlab-ci-stag.yml          # Staging deployment pipeline
├── tests/
│   ├── __init__.py                  # Tests package
│   └── test_api.py                  # API endpoint tests
├── Dockerfile                       # Docker container configuration
├── requirements.txt                 # Production dependencies
├── requirements-dev.txt             # Development dependencies
├── .gitlab-ci.yml                   # Main CI/CD configuration
├── .gitignore                       # Git ignore rules
├── .dockerignore                    # Docker ignore rules
├── LICENSE                          # Project license
├── template.py                      # Repository template generator
└── README.md                        # This file
```

## ⚙️ Installation

## 🛠️ Prerequisites

- Python 3.12+
- Google Cloud Project with Vertex AI enabled
- Service account key for Google Cloud authentication

### ⚙️ Setup

1. **Clone the repository:**
```bash
git clone https://github.com/Amirazizgithub/Insight-Generation-Model.git
```
```bash
cd spinotale-insight-generation
```

2. **Create virtual environment:**
```bash
python -m venv venv
```
```bash
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Set up environment variables:**
   Create a .env file in the root directory:
   `env`
```bash
LLM_MODEL=gemini-2.5-flash
PROJECT_ID=your-gcp-project-id
PROJECT_REGION=your-gcp-region
```

5. **Set up Google Cloud authentication:**
   Place your service account key file as central-sa-key.json in the root directory.

## 🏃‍♂️ Usage

### Running the Application

Start the FastAPI server with auto-reload:
```bash
uvicorn app:app --reload
```

The API will be available at http://127.0.0.1:8000

### API Documentation

Once running, visit http://127.0.0.1:8000/docs for interactive Swagger UI documentation.

### API Endpoints

#### Health Check
- **GET** / - Welcome message
- **GET** api/v1/check_health - API health status

#### CDP Insights
- **POST** api/v1/cdp/generate_insight_l1 - Generate Level 1 insights from CDP card data
- **POST** api/v1/cdp/generate_insight_l2 - Generate Level 2 insights from CDP time-series data

#### Intelligence Insights
- **POST** /intelligence/generate_insight_l1 - Generate Level 1 insights from Intelligence dashboard data
- **POST** /intelligence/generate_insight_l2 - Generate Level 2 insights from Intelligence time-series data

### 🏃‍♂️ Example API Usage

#### CDP L1 Insight Generation
```bash
curl -X POST "http://127.0.0.1:8000/api/v1/cdp/generate_insight_l1" \
     -H "Content-Type: application/json" \
     -d '{
       "leads": {"total": 1000},
       "Customers": {"total": 250},
       "Miscellaneous": {"total": 2000}
     }'
```

#### CDP L2 Insight Generation
```bash
curl -X POST "http://127.0.0.1:8000/api/v1/cdp/generate_insight_l2?start_date_str=2024-01-01&end_date_str=2024-01-31" \
     -H "Content-Type: application/json" \
     -d '[{... time-series data ...}]'
```

#### Intelligence L1 Insight Generation
```bash
curl -X POST "http://127.0.0.1:8000/api/v1/intelligence/generate_insight_l1" \
     -H "Content-Type: application/json" \
     -d '[{... data ...}]'
```

#### Intelligence L2 Insight Generation
```bash
curl -X POST "http://127.0.0.1:8000/api/v1/intelligence/generate_insight_l2" \
     -H "Content-Type: application/json" \
     -d '{
       "industry": "ecommerce",
       "domain": "marketing",
       "dimension_col": "date",
       "window": 30
     }`
```

## Testing

Run the test suite:
```bash
pytest
```

Run with coverage:
```bash
pytest --cov=app --cov-report=html
```

## 🐳 Docker Deployment

### Build the Docker image:
```bash
docker build -t insight-generation-model-{ENVIRONMENT}:latest .
```

### Run the container:
```bash
docker run -p 8000:8000 insight-generation-model-{ENVIRONMENT}:latest
```

## 🛠️ CI/CD Pipelines

The project includes GitLab CI/CD pipelines for:

- **Development**: Code formatting and linting
- **Staging**: Automated testing and deployment
- **Production**: Full deployment pipeline

Pipelines are triggered based on branch:
- development branch: Development pipeline
- staging branch: Staging deployment
- production branch: Production deployment

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| LLM_MODEL | Vertex AI model to use | gemini-2.5-flash |
| PROJECT_ID | Google Cloud Project ID | Required |
| PROJECT_REGION | Google Cloud Region | Required |

### Logging

The application uses structured logging configured in app/utils/logger.py. Logs include:
- API request/response details
- Error tracking
- Performance metrics

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: git checkout -b feature/your-feature
3. Make your changes and add tests
4. Run tests: pytest
5. Format code: black .
6. Commit your changes: git commit -am 'Add your feature'
7. Push to the branch: git push origin feature/your-feature
8. Submit a pull request

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## ⚙️ Support

For support or questions:
- Create an issue in the repository
- Contact the development team

## Roadmap

- [ ] Enhanced data validation
- [ ] Additional AI model integrations
- [ ] Real-time insight streaming
- [ ] Advanced analytics dashboard
- [ ] Multi-language support
