import os
import oci
import json
from datetime import datetime

def send_completion_message(logger):
    """
    작업 완료 메시지를 OCI Queue에 전송합니다.
    인증을 위한 signer 객체와 로깅을 위한 logger 객체를 인자로 받습니다.
    """
    try:
        logger.info("큐 메시지 전송을 시작합니다.")
        
        logger.info("시그널 정보를 가져옵니다.")
        signer = oci.auth.signers.get_resource_principals_signer()
        
        # 환경 변수에서 큐 정보 가져오기
        queue_id = os.environ.get("QUEUE_ID")
        queue_endpoint = os.environ.get("QUEUE_ENDPOINT")
        
        # OCI Queue 클라이언트 초기화
        queue_client = oci.queue.QueueClient(config={}, signer=signer, service_endpoint=queue_endpoint)
        
        if not all([queue_id, queue_endpoint]):
            raise ValueError("Queue 관련 환경 변수(QUEUE_ID, QUEUE_ENDPOINT)가 설정되지 않았습니다.")

        # 보낼 메시지 내용 정의
        message_content = json.dumps({
            "status": "SUCCESS",
            "source": "data-collection-function",
            "created_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        message_entries = [
            oci.queue.models.PutMessagesDetailsEntry(content=message_content)
        ]
        
        put_messages_details_object = oci.queue.models.PutMessagesDetails(messages=message_entries)
        
        # 큐에 메시지 넣기
        put_messages_response = queue_client.put_messages(
            queue_id=queue_id,
            put_messages_details=put_messages_details_object
        )
        
        # 메시지 전송 결과 확인
        if hasattr(put_messages_response.data, 'failures') and put_messages_response.data.failures:
            # 실패한 메시지가 있을 경우에만 이 블록이 실행됨
            failed_message = put_messages_response.data.failures[0]
            raise Exception(f"메시지 전송 실패: {failed_message.message}")
        else:
            # 실패한 메시지가 없으면 성공으로 간주
            logger.info("모든 메시지가 큐에 성공적으로 전송되었습니다.")
            logger.info(f"전송 결과: {put_messages_response.data.messages}")

        logger.info("큐에 메시지를 성공적으로 전송했습니다.")

    except Exception as e:
        logger.error(f"큐 메시지 전송 중 오류 발생: {e}", exc_info=True)
        # 에러를 다시 발생시켜 상위 핸들러가 인지하고 처리하도록 함
        raise