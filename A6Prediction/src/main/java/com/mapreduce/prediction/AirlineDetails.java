package com.mapreduce.prediction;

import org.apache.commons.lang.StringUtils;

/**
 * @author Pankaj Tripathi, Kartik Mahaley, Shakti Patro
 * */
public class AirlineDetails {

	private String year;
	private String month;
	private Integer dayOfMonth;
	private Integer dayOfWeek;
	private String carrier;
	private String flDate;
	private String flNum;
	private String origin;
	private String destination;
	private Integer originAirportId;
	private Integer originAirportSequenceId;
	private Integer originCityMarketId;
	private Integer originStateFips;
	private Integer originWac;
	private String originCityName;
	private String originStateAbbr;
	private String originStateName;
	private Integer destinationAirportId;
	private Integer destinationAirportSequenceId;
	private Integer destinationCityMarketId;
	private Integer destinationStateFips;
	private Integer destinationWac;
	private String destinationCityName;
	private String destinationStateAbbr;
	private String destinationStateName;
	private int crsArrivalTime;
	private int crsDepartureTime;
	private int crsElapsedTime;
	private int actualArrivalTime;
	private int actualDepartureTime;
	private int actualElapsedTime;
	private double departureDelay;
	private double arrivalDelay;
	private double arrivalDelayMinutes;
	private double arrivalDelay15;
	private int cancelled;
	private double price;

	public Integer getDayOfMonth() {
		return dayOfMonth;
	}

	public void setDayOfMonth(Integer dayOfMonth) {
		this.dayOfMonth = dayOfMonth;
	}

	public Integer getDayOfWeek() {
		return dayOfWeek;
	}

	public void setDayOfWeek(Integer dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}

	public String getFlDate() {
		return flDate;
	}

	public void setFlDate(String flDate) {
		this.flDate = flDate;
	}

	public String getFlNum() {
		return flNum;
	}

	public void setFlNum(String flNum) {
		this.flNum = flNum;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getCarrier() {
		return carrier;
	}

	public void setCarrier(String carrier) {
		this.carrier = carrier;
	}

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public String getDestinationCityName() {
		return destinationCityName;
	}

	public void setDestinationCityName(String destinationCityName) {
		this.destinationCityName = destinationCityName;
	}

	public Integer getOriginAirportId() {
		return originAirportId;
	}

	public void setOriginAirportId(Integer originAirportId) {
		this.originAirportId = originAirportId;
	}

	public Integer getOriginAirportSequenceId() {
		return originAirportSequenceId;
	}

	public void setOriginAirportSequenceId(Integer originAirportSequenceId) {
		this.originAirportSequenceId = originAirportSequenceId;
	}

	public Integer getOriginCityMarketId() {
		return originCityMarketId;
	}

	public void setOriginCityMarketId(Integer originCityMarketId) {
		this.originCityMarketId = originCityMarketId;
	}

	public Integer getOriginStateFips() {
		return originStateFips;
	}

	public void setOriginStateFips(Integer originStateFips) {
		this.originStateFips = originStateFips;
	}

	public Integer getOriginWac() {
		return originWac;
	}

	public void setOriginWac(Integer originWac) {
		this.originWac = originWac;
	}

	public String getOriginCityName() {
		return originCityName;
	}

	public void setOriginCityName(String originCityName) {
		this.originCityName = originCityName;
	}

	public String getOriginStateAbbr() {
		return originStateAbbr;
	}

	public void setOriginStateAbbr(String originStateAbbr) {
		this.originStateAbbr = originStateAbbr;
	}

	public String getOriginStateName() {
		return originStateName;
	}

	public void setOriginStateName(String originStateName) {
		this.originStateName = originStateName;
	}

	public Integer getDestinationAirportId() {
		return destinationAirportId;
	}

	public void setDestinationAirportId(Integer destinationAirportId) {
		this.destinationAirportId = destinationAirportId;
	}

	public Integer getDestinationAirportSequenceId() {
		return destinationAirportSequenceId;
	}

	public void setDestinationAirportSequenceId(Integer destinationAirportSequenceId) {
		this.destinationAirportSequenceId = destinationAirportSequenceId;
	}

	public Integer getDestinationCityMarketId() {
		return destinationCityMarketId;
	}

	public void setDestinationCityMarketId(Integer destinationCityMarketId) {
		this.destinationCityMarketId = destinationCityMarketId;
	}

	public Integer getDestinationStateFips() {
		return destinationStateFips;
	}

	public void setDestinationStateFips(Integer destinationStateFips) {
		this.destinationStateFips = destinationStateFips;
	}

	public Integer getDestinationWac() {
		return destinationWac;
	}

	public void setDestinationWac(Integer destinationWac) {
		this.destinationWac = destinationWac;
	}

	public String getDestinationName() {
		return destinationCityName;
	}

	public void setDestinationName(String destinationName) {
		this.destinationCityName = destinationName;
	}

	public String getDestinationStateAbbr() {
		return destinationStateAbbr;
	}

	public void setDestinationStateAbbr(String destinationStateAbbr) {
		this.destinationStateAbbr = destinationStateAbbr;
	}

	public String getDestinationStateName() {
		return destinationStateName;
	}

	public void setDestinationStateName(String destinationStateName) {
		this.destinationStateName = destinationStateName;
	}

	public int getCrsArrivalTime() {
		return crsArrivalTime;
	}

	public void setCrsArrivalTime(int crsArrivalTime) {
		this.crsArrivalTime = crsArrivalTime;
	}

	public int getCrsDepartureTime() {
		return crsDepartureTime;
	}

	public void setCrsDepartureTime(int crsDepartureTime) {
		this.crsDepartureTime = crsDepartureTime;
	}

	public int getCrsElapsedTime() {
		return crsElapsedTime;
	}

	public void setCrsElapsedTime(int crsElapsedTime) {
		this.crsElapsedTime = crsElapsedTime;
	}

	public int getActualArrivalTime() {
		return actualArrivalTime;
	}

	public void setActualArrivalTime(int actualArrivalTime) {
		this.actualArrivalTime = actualArrivalTime;
	}

	public int getActualDepartureTime() {
		return actualDepartureTime;
	}

	public void setActualDepartureTime(int actualDepartureTime) {
		this.actualDepartureTime = actualDepartureTime;
	}

	public int getActualElapsedTime() {
		return actualElapsedTime;
	}

	public void setActualElapsedTime(int actualElapsedTime) {
		this.actualElapsedTime = actualElapsedTime;
	}

	public double getDepartureDelay() {
		return departureDelay;
	}

	public void setDepartureDelay(double departureDelay) {
		this.departureDelay = departureDelay;
	}

	public double getArrivalDelay() {
		return arrivalDelay;
	}

	public void setArrivalDelay(double arrivalDelay) {
		this.arrivalDelay = arrivalDelay;
	}

	public double getArrivalDelayMinutes() {
		return arrivalDelayMinutes;
	}

	public void setArrivalDelayMinutes(double arrivalDelayMinutes) {
		this.arrivalDelayMinutes = arrivalDelayMinutes;
	}

	public double getArrivalDelay15() {
		return arrivalDelay15;
	}

	public void setArrivalDelay15(double arrivalDelay15) {
		this.arrivalDelay15 = arrivalDelay15;
	}

	public int getCancelled() {
		return cancelled;
	}

	public void setCancelled(int cancelled) {
		this.cancelled = cancelled;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	@Override
	public String toString() {
		return "AirlineDetails [year=" + year + ", month=" + month + ", carrier=" + carrier + ", origin=" + origin
				+ ", destination=" + destination + ", originAirportId=" + originAirportId + ", originAirportSequenceId="
				+ originAirportSequenceId + ", originCityMarketId=" + originCityMarketId + ", originStateFips="
				+ originStateFips + ", originWac=" + originWac + ", originCityName=" + originCityName
				+ ", originStateAbbr=" + originStateAbbr + ", originStateName=" + originStateName
				+ ", destinationAirportId=" + destinationAirportId + ", destinationAirportSequenceId="
				+ destinationAirportSequenceId + ", destinationCityMarketId=" + destinationCityMarketId
				+ ", destinationStateFips=" + destinationStateFips + ", destinationWac=" + destinationWac
				+ ", destinationCityName=" + destinationCityName + ", destinationStateAbbr=" + destinationStateAbbr
				+ ", destinationStateName=" + destinationStateName + ", crsArrivalTime=" + crsArrivalTime
				+ ", crsDepartureTime=" + crsDepartureTime + ", crsElapsedTime=" + crsElapsedTime
				+ ", actualArrivalTime=" + actualArrivalTime + ", actualDepartureTime=" + actualDepartureTime
				+ ", actualElapsedTime=" + actualElapsedTime + ", arrivalDelay=" + arrivalDelay
				+ ", arrivalDelayMinutes=" + arrivalDelayMinutes + ", arrivalDelay15=" + arrivalDelay15 + ", cancelled="
				+ cancelled + ", price=" + price + ", flDate=" + flDate + ", flNum=" + flNum 
				+ ", dayOfMonth=" + dayOfMonth + ", dayOfWeek=" + dayOfWeek + "]";
	}

	/*
	 * Constructor : Takes an array of flight details with size 110. Note: Size
	 * is assumed to be validated before constructor call.
	 */
	public AirlineDetails(String[] flightDetails) throws InvalidFormatException {
		super();
		try {
			this.year = flightDetails[0];
			this.month = flightDetails[2];
			this.dayOfMonth = Integer.parseInt(flightDetails[3]);
			this.dayOfWeek = Integer.parseInt(flightDetails[4]);
			this.flDate = flightDetails[5];
			this.flNum = flightDetails[10];
			this.carrier = flightDetails[8];
			this.origin = flightDetails[14];
			this.destination = flightDetails[23];
			this.originAirportId = Integer.parseInt(flightDetails[11]);
			this.originAirportSequenceId = Integer.parseInt(flightDetails[12]);
			this.originCityMarketId = Integer.parseInt(flightDetails[13]);
			this.originStateFips = Integer.parseInt(flightDetails[17]);
			this.originWac = Integer.parseInt(flightDetails[19]);
			this.originCityName = flightDetails[15];
			this.originStateAbbr = flightDetails[16];
			this.originStateName = flightDetails[18];
			this.destinationAirportId = Integer.parseInt(flightDetails[20]);
			this.destinationAirportSequenceId = Integer.parseInt(flightDetails[21]);
			this.destinationCityMarketId = Integer.parseInt(flightDetails[22]);
			this.destinationStateFips = Integer.parseInt(flightDetails[26]);
			this.destinationWac = Integer.parseInt(flightDetails[28]);
			this.destinationCityName = flightDetails[24];
			this.destinationStateAbbr = flightDetails[25];
			this.destinationStateName = flightDetails[27];
			if (StringUtils.isNotEmpty(flightDetails[41]) && StringUtils.isNotEmpty(flightDetails[30])
					&& StringUtils.isNotEmpty(flightDetails[51])) {
				this.crsArrivalTime = Integer.parseInt(flightDetails[40]);
				this.crsDepartureTime = Integer.parseInt(flightDetails[29]);
				this.crsElapsedTime = Integer.parseInt(flightDetails[50]);
				this.actualArrivalTime = Integer.parseInt(flightDetails[41]);
				this.actualDepartureTime = Integer.parseInt(flightDetails[30]);
				this.actualElapsedTime = Integer.parseInt(flightDetails[51]);
			}
			if (StringUtils.isNotEmpty(flightDetails[31])) {
				this.departureDelay = Double.parseDouble(flightDetails[31]);
			}
			if (StringUtils.isNotEmpty(flightDetails[42])) {
				this.arrivalDelay = Double.parseDouble(flightDetails[42]);
				this.arrivalDelayMinutes = Double.parseDouble(flightDetails[43]);
				this.arrivalDelay15 = Double.parseDouble(flightDetails[44]);
			}
			this.cancelled = Integer.parseInt(flightDetails[47]);
			this.price = Double.parseDouble(flightDetails[109]);
		} catch (NumberFormatException e) {
			throw new InvalidFormatException("String in place of a number.");
		} catch (NullPointerException e) {
			throw new InvalidFormatException("Null got where not expected.");
		}
	}

	/*
	 * Constructor : Takes an array of flight details with size 110. Note: Size
	 * is assumed to be validated before constructor call.
	 */
	public AirlineDetails(String[] flightDetails, boolean val) throws InvalidFormatException {
		super();
		try {
			this.year = flightDetails[0];
			this.month = flightDetails[2];
			this.dayOfMonth = Integer.parseInt(flightDetails[3]);
			this.dayOfWeek = Integer.parseInt(flightDetails[4]);
			this.flDate = flightDetails[5];
			this.flNum = flightDetails[10];
			this.carrier = flightDetails[8];
			this.origin = flightDetails[14];
			this.destination = flightDetails[23];
			if (StringUtils.isNotEmpty(flightDetails[41]) && StringUtils.isNotEmpty(flightDetails[30])
					&& StringUtils.isNotEmpty(flightDetails[51]) && !StringUtils.equals(flightDetails[30], "NA")
					&& !StringUtils.equals(flightDetails[41], "NA")&& !StringUtils.equals(flightDetails[51], "NA")) {
				this.actualArrivalTime = Integer.parseInt(flightDetails[41]);
				this.actualDepartureTime = Integer.parseInt(flightDetails[30]);
				this.actualElapsedTime = Integer.parseInt(flightDetails[51]);
			}
			this.crsArrivalTime = Integer.parseInt(flightDetails[40]);
			this.crsElapsedTime = Integer.parseInt(flightDetails[50]);
			this.crsDepartureTime = Integer.parseInt(flightDetails[29]);
			if (StringUtils.isNotEmpty(flightDetails[31]) && !flightDetails[31].equals("NA")) {
				this.departureDelay = Double.parseDouble(flightDetails[31]);
			}
			if (StringUtils.isNotEmpty(flightDetails[42]) && !flightDetails[42].equals("NA")) {
				this.arrivalDelay = Double.parseDouble(flightDetails[42]);
				this.arrivalDelayMinutes = Double.parseDouble(flightDetails[43]);
				this.arrivalDelay15 = Double.parseDouble(flightDetails[44]);
			}
		} catch (NumberFormatException e) {
			throw new InvalidFormatException("String in place of a number.");
		} catch (NullPointerException e) {
			throw new InvalidFormatException("Null got where not expected.");
		}
	}
}
