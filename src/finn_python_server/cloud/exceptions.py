class DataPipelineError(Exception):
    """데이터 파이프라인 관련 최상위 예외 클래스"""
    def __init__(self, message="데이터 파이프라인 처리 중 오류가 발생했습니다."):
        self.message = message
        super().__init__(self.message)

class ConfigError(DataPipelineError):
    """환경 변수 등 설정 관련 오류"""
    pass

class ApiError(DataPipelineError):
    """외부 API 연동 관련 오류"""
    pass

class TiingoApiError(ApiError):
    """Tiingo API 관련 오류"""
    pass

class DbError(DataPipelineError):
    """데이터베이스 관련 오류"""
    pass

class SupabaseError(DbError):
    """Supabase DB 관련 오류"""
    pass

class DataProcessingError(DataPipelineError):
    """데이터 처리/가공 중 발생하는 오류"""
    pass