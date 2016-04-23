package com.cs6240.airline;

import java.io.Serializable;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;

import com.cs6240.exception.InvalidFormatException;


/*
 * author: Shakti Patro , Kavyashree nagendrakumar
 */

/*
 * Class : Encapsulates all airlines realted data
 */
public class AirlineDetailsPojo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Integer year;
	private Integer month;
	private String carrier;
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
	private double arrivalDelay;
	private double arrivalDelayMinutes;
	private double arrivalDelay15;
	private int cancelled;
	private double price;
	
	public Integer getYear() {
		return year;
	}
	public void setYear(Integer year) {
		this.year = year;
	}
	public Integer getMonth() {
		return month;
	}
	public void setMonth(Integer month) {
		this.month = month;
	}
//	public Date getFlightDate() {
//		return flightDate;
//	}
//	public void setFlightDate(Date flightDate) {
//		this.flightDate = flightDate;
//	}
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
		return "AirlineDetails [year=" + year + ", month=" + month
				+ ", carrier=" + carrier + ", origin=" + origin
				+ ", destination=" + destination + ", originAirportId="
				+ originAirportId + ", originAirportSequenceId="
				+ originAirportSequenceId + ", originCityMarketId="
				+ originCityMarketId + ", originStateFips=" + originStateFips
				+ ", originWac=" + originWac + ", originCityName="
				+ originCityName + ", originStateAbbr=" + originStateAbbr
				+ ", originStateName=" + originStateName
				+ ", destinationAirportId=" + destinationAirportId
				+ ", destinationAirportSequenceId="
				+ destinationAirportSequenceId + ", destinationCityMarketId="
				+ destinationCityMarketId + ", destinationStateFips="
				+ destinationStateFips + ", destinationWac=" + destinationWac
				+ ", destinationCityName=" + destinationCityName
				+ ", destinationStateAbbr=" + destinationStateAbbr
				+ ", destinationStateName=" + destinationStateName
				+ ", crsArrivalTime=" + crsArrivalTime + ", crsDepartureTime="
				+ crsDepartureTime + ", crsElapsedTime=" + crsElapsedTime
				+ ", actualArrivalTime=" + actualArrivalTime
				+ ", actualDepartureTime=" + actualDepartureTime
				+ ", actualElapsedTime=" + actualElapsedTime
				+ ", arrivalDelay=" + arrivalDelay + ", arrivalDelayMinutes="
				+ arrivalDelayMinutes + ", arrivalDelay15=" + arrivalDelay15
				+ ", cancelled=" + cancelled + ", price=" + price + "]";
	}
	
	/*
	 * Constructor : Takes an array of flight details with size 110.
	 * This constructor sets in values by the column numbers 
	 * Note: Size is assumed to be validated before constructor call.
	 */
	public AirlineDetailsPojo(String[] flightDetails) throws InvalidFormatException{
		super();
		try{
			this.year = Integer.parseInt(flightDetails[0]);
			this.month = Integer.parseInt(flightDetails[2]);
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
			if(StringUtils.isNotEmpty(flightDetails[41]) && StringUtils.isNotEmpty(flightDetails[30]) && StringUtils.isNotEmpty(flightDetails[51])) {
				this.crsArrivalTime = Integer.parseInt(flightDetails[40]);
				this.crsDepartureTime = Integer.parseInt(flightDetails[29]);
				this.crsElapsedTime = Integer.parseInt(flightDetails[50]);
				this.actualArrivalTime = Integer.parseInt(flightDetails[41]);
				this.actualDepartureTime = Integer.parseInt(flightDetails[30]);
				this.actualElapsedTime = Integer.parseInt(flightDetails[51]);
			}
			
			if(StringUtils.isNotEmpty(flightDetails[42])) {
				this.arrivalDelay = Double.parseDouble(flightDetails[42]);
				this.arrivalDelayMinutes = Double.parseDouble(flightDetails[43]);
				this.arrivalDelay15 = Double.parseDouble(flightDetails[44]);
			}
			this.cancelled = Integer.parseInt(flightDetails[47]);
			this.price = Double.parseDouble(flightDetails[109]);

		} catch (NumberFormatException e) {
			System.out.println("##########ERROR FORMAT:::::::::::");
			e.printStackTrace();
			throw new InvalidFormatException("String in place of a number.");
		} catch (NullPointerException e) {
			throw new InvalidFormatException("Null got where not expected.");
		}
	}
	
	
	/*
	 * Constructor : Takes a CSVRecord of flight details with size 110.
	 * This constructor sets in values into the object by te header names 
	 */
	public AirlineDetailsPojo(CSVRecord record) throws InvalidFormatException{
		super();
		try{
			this.year = Integer.parseInt(record.get(AirlineConstants.YEAR));
			this.month = Integer.parseInt(record.get(AirlineConstants.MONTH));
			this.carrier = record.get(AirlineConstants.CARRIER);
			this.origin = record.get(AirlineConstants.ORIGIN);
			this.destination = record.get(AirlineConstants.DEST);
			this.originAirportId = Integer.parseInt(record.get(AirlineConstants.ORIGIN_AIRPORT_ID));
			this.originAirportSequenceId = Integer.parseInt(record.get(AirlineConstants.ORIGIN_AIRPORT_SEQ_ID));
			this.originCityMarketId = Integer.parseInt(record.get(AirlineConstants.ORIGIN_CITY_MARKET_ID));
			this.originStateFips = Integer.parseInt(record.get(AirlineConstants.ORIGIN_STATE_FIPS));
			this.originWac = Integer.parseInt(record.get(AirlineConstants.ORIGIN_WAC));
			this.originCityName = record.get(AirlineConstants.ORIGIN_CITY_NAME);
			this.originStateAbbr = record.get(AirlineConstants.ORIGIN_STATE_ABR);
			this.originStateName = record.get(AirlineConstants.ORIGIN_STATE_NM);
			this.destinationAirportId = Integer.parseInt(record.get(AirlineConstants.DEST_AIRPORT_ID));
			this.destinationAirportSequenceId = Integer.parseInt(record.get(AirlineConstants.DEST_AIRPORT_SEQ_ID));
			this.destinationCityMarketId = Integer.parseInt(record.get(AirlineConstants.DEST_CITY_MARKET_ID));
			this.destinationStateFips = Integer.parseInt(record.get(AirlineConstants.DEST_STATE_FIPS));
			this.destinationWac = Integer.parseInt(record.get(AirlineConstants.DEST_WAC));
			this.destinationCityName = record.get(AirlineConstants.DEST_CITY_NAME);
			this.destinationStateAbbr = record.get(AirlineConstants.DEST_STATE_ABR);
			this.destinationStateName = record.get(AirlineConstants.DEST_STATE_NM);
			if(StringUtils.isNotEmpty(record.get(AirlineConstants.ARR_TIME)) 
					&& StringUtils.isNotEmpty(record.get(AirlineConstants.DEP_TIME)) 
					&& StringUtils.isNotEmpty(record.get(AirlineConstants.ACTUAL_ELAPSED_TIME))) {
				this.crsArrivalTime = Integer.parseInt(record.get(AirlineConstants.CRS_ARR_TIME));
				this.crsDepartureTime = Integer.parseInt(record.get(AirlineConstants.CRS_DEP_TIME));
				this.crsElapsedTime = Integer.parseInt(record.get(AirlineConstants.CRS_ELAPSED_TIME));
				this.actualArrivalTime = Integer.parseInt(record.get(AirlineConstants.ARR_TIME));
				this.actualDepartureTime = Integer.parseInt(record.get(AirlineConstants.DEP_TIME));
				this.actualElapsedTime = Integer.parseInt(record.get(AirlineConstants.ACTUAL_ELAPSED_TIME));
			}
			
			if(StringUtils.isNotEmpty(record.get(AirlineConstants.ARR_DELAY))) {
				this.arrivalDelay = Double.parseDouble(record.get(AirlineConstants.ARR_DELAY));
				this.arrivalDelayMinutes = Double.parseDouble(record.get(AirlineConstants.ARR_DELAY_NEW));
				this.arrivalDelay15 = Double.parseDouble(record.get(AirlineConstants.ARR_DEL15));
			}
			this.cancelled = Integer.parseInt(record.get(AirlineConstants.CANCELLED));
			this.price = Double.parseDouble(record.get(AirlineConstants.AVG_TICKET_PRICE));

		} catch (NumberFormatException e) {
			System.out.println("##########ERROR FORMAT:::::::::::");
			e.printStackTrace();
			throw new InvalidFormatException("String in place of a number.");
		} catch (NullPointerException e) {
			throw new InvalidFormatException("Null got where not expected.");
		}
	}
	
}
