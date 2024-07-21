/*
 * Copyright (c) 2019-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.micro

/** In-memory cache containing the results of the validation (or not) of the tracking events.
  * Good events are stored with their type, their schema and their contexts, if any,
  * so that they can be quickly filtered.
  * Bad events are stored with the error message(s) describing what when wrong.
  */

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import java.sql.{DriverManager}
import io.circe.syntax._
import io.circe.parser._
import scala.collection.mutable.ListBuffer
import io.circe.{ Decoder }
import java.time.Instant
import scala.util.Try
import io.circe.generic.semiauto.deriveDecoder
import java.time.{LocalDateTime, ZoneId, Instant}
import java.time.format.DateTimeFormatter
import scala.util.Try

private[micro] trait ValidationCache {
  import ValidationCache._

  protected var good: List[GoodEvent]
  private object LockGood
  protected var bad: List[BadEvent]
  private object LockBad

  val conn = DriverManager.getConnection("jdbc:duckdb:")

  implicit val decodeInstant: Decoder[Instant] = Decoder.decodeString.emapTry { str =>
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    Try {
      Try(Instant.parse(str)) // First, try parsing the string as an Instant directly.
        .getOrElse {
          // If the direct parsing fails, try parsing it as a LocalDateTime with a custom formatter.
          val localDateTime = LocalDateTime.parse(str, formatter)
          // Convert LocalDateTime to Instant using the system's default time zone.
          // You might want to specify a specific time zone if the default is not appropriate.
          localDateTime.atZone(ZoneId.systemDefault()).toInstant
        }
    }
  }


  /** Compute a summary with the number of good and bad events currently in cache. */
  private[micro] def getSummary(): ValidationSummary = {
    val nbGood = LockGood.synchronized {
      good.size
    }
    val nbBad = LockBad.synchronized {
      bad.size
    }
    ValidationSummary(nbGood + nbBad, nbGood, nbBad)
  }

  /** Add a good event to the cache. */
  private[micro] def addToGood(events: List[GoodEvent]): Unit =
    LockGood.synchronized {
      good = events ++ good

      // convert events into a json representation
      val x = events.map(_.event.toJson(false))
      println(events.map(_.event.asJson))
      println(x)

      println(x)
      // add an event to duckdb?
      val stmt = conn.createStatement();
      // note rawevent is missing because I do not care about it.
       stmt.execute("CREATE TABLE IF NOT EXISTS good (event_type VARCHAR, schema VARCHAR, contexts VARCHAR[], event STRUCT(app_id VARCHAR, collector_tstamp TIMESTAMP, event_id VARCHAR, v_collector VARCHAR, v_etl VARCHAR))")
      stmt.close()


      var insertstmt = conn.prepareStatement("INSERT INTO good VALUES ('test', 'test', ARRAY['test'], ROW(?, ?, ?, ?, ?));")
      // a row with the values?
      events.headOption.foreach { firstEvent =>
        insertstmt.setString(1, firstEvent.event.app_id.getOrElse(null))
        insertstmt.setString(2, "2024-01-01T00:00:00.000Z")
        insertstmt.setString(3, "123")
        insertstmt.setString(4, "123")
        insertstmt.setString(5, "123")
      }
      insertstmt.execute()
      insertstmt.close()


    }

  /** Add a bad event to the cache. */
  private[micro] def addToBad(events: List[BadEvent]): Unit =
    LockBad.synchronized {
      bad = events ++ bad
    }



  /** Remove all the events from memory. */
  private[micro] def reset(): Unit = {
    LockGood.synchronized {
      good = List.empty[GoodEvent]
    }
    LockBad.synchronized {
      bad = List.empty[BadEvent]
    }
  }

  private [micro] def filterSQL(query: Option[String] = None): List[Event] = {
    implicit val eventDecoder: Decoder[Event] = deriveDecoder[Event]
    val stmt = conn.createStatement()
    println("filter recv:", query)
    val result = stmt.executeQuery("SELECT TO_JSON(event) FROM good")

    val goodEventsBuffer = ListBuffer[Event]()
    // define a new list of strings
    while (result.next()) {
      val o = result.getString(1) // might need to decode here?
      // val x = parse(o)
      val x = decode[Event](o)
      println("parsed:", x)
      x match {
        // case Right(Event(_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_)) => {
        case Right(event) => {
          println("success:")
          goodEventsBuffer += event
        }
        case Left(error) => {
          println("error:", error)
        }

    }
      // append to goodEvents
      
    }
    stmt.close()
    goodEventsBuffer.toList // this ends up getting double encoded (which is not ideal)
  }

  /** Filter out the good events with the possible filters contained in the HTTP request. */
  private[micro] def filterGood(
    filtersGood: FiltersGood = FiltersGood(None, None, None, None)
  ): List[GoodEvent] = {

    LockGood.synchronized {
      // allow any combination of filters here

      val filtered = good.filter(keepGoodEvent(_, filtersGood))
      filtered.take(filtersGood.limit.getOrElse(filtered.size))
    }

    // I don't know how a list of filtered good events becomes a JSON object?

  }


  /** Filter out the bad events with the possible filters contained in the HTTP request. */
  private[micro] def filterBad(
    filtersBad: FiltersBad = FiltersBad(None, None, None)
  ): List[BadEvent] =
    LockBad.synchronized {
      val filtered = bad.filter(keepBadEvent(_, filtersBad))
      filtered.take(filtersBad.limit.getOrElse(filtered.size))
    }

}

private[micro] object ValidationCache extends ValidationCache {
  protected var good = List.empty[GoodEvent]
  protected var bad = List.empty[BadEvent]

  /** Check if a good event matches the filters. */
  private[micro] def keepGoodEvent(event: GoodEvent, filters: FiltersGood): Boolean =
    filters.event_type.toSet.subsetOf(event.eventType.toSet) &&
      filters.schema.toSet.subsetOf(event.schema.toSet) &&
      filters.contexts.forall(containsAllContexts(event, _))

  /** Check if an event conntains all the contexts of the list. */
  private[micro] def containsAllContexts(event: GoodEvent, contexts: List[String]): Boolean =
    contexts.forall(event.contexts.contains)

  /** Check if a bad event matches the filters. */
  private[micro] def keepBadEvent(event: BadEvent, filters: FiltersBad): Boolean =
    filters.vendor.forall(vendor => event.collectorPayload.forall(_ .api.vendor == vendor)) &&
      filters.version.forall(version => event.collectorPayload.forall(_ .api.version == version))
}
