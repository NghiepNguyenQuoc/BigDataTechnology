package cs523.AnalysicLogFile;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogEnTry{
	private String ip;
	private String date;
	private int code;
	private  static String logEntryPattern = "^(\\S+).+\\[(\\S+\\s.\\d{4})\\] .+\\\" (\\d{3}) \\S+";
	private static Pattern pattern = Pattern.compile(logEntryPattern);
	static DateTimeFormatter df = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");
	
	
	public LogEnTry (String ip, String date, String code){
		this.ip = ip;
		this.date = date;
		this.code = Integer.parseInt(code);
	}
	
	public static LogEnTry parser(String line){
		Matcher match = pattern.matcher(line);
		if (!match.find())
			return null;
		else
			return new LogEnTry(match.group(1),match.group(2),match.group(3));
		
		
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public LocalDateTime getDate() {
		return LocalDateTime.parse(date, df);
	}

	public void setDate(String date) {
		this.date = date;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

}
