package com.jeong.hadoop;

import org.apache.hadoop.io.Text;

public class AirParser {
	private String uniqueCarrier_sol;     //항공사 코드
	private String tailNum_sol;  		 // 항공기 등록번호
	private int carrierDelay_sol;  		// 항공사 지연시간 (분 단위)
	
	final static int NONDELAY_SOL = 0;
	
	private int getDigitFromStr(String str, int defaultDigit) {  // 항공기 데이터 값에 NA가 있을 경우의 처리
		if("NA".equalsIgnoreCase(str)) {
			return defaultDigit;   // if 조건에 맞는 return값 
		}
		return Integer.parseInt(str);   //if 조건에 맞지 않는 경우 return
	}
	
	public AirParser() {  // default 생성자
	}
	
	public AirParser(Text value) { // 입력값이 있는 생성자
		String airData[] = value.toString().split(",");  // , 를 기준으로 단어를 끊어줌
		
		uniqueCarrier_sol = airData[8];
		tailNum_sol = airData[10];
		carrierDelay_sol = getDigitFromStr(airData[24], NONDELAY_SOL);
	}
	
	public String getUniqueCarrier_sol() {
		return uniqueCarrier_sol;
	}

	public String getTailNum_sol() {
		return tailNum_sol;
	}

	public int getCarrierDelay_sol() {
		return carrierDelay_sol;
	}

	public void setUniqueCarrier_sol(String uniqueCarrier_sol) {
		this.uniqueCarrier_sol = uniqueCarrier_sol;
	}

	public void setTailNum_sol(String tailNum_sol) {
		this.tailNum_sol = tailNum_sol;
	}

	public void setCarrierDelay_sol(int carrierDelay_sol) {
		this.carrierDelay_sol = carrierDelay_sol;
	}
	
	
	
	
}
