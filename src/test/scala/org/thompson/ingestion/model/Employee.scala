package org.thompson.ingestion.model

final case class Employee(id: Long, name: String, department: String)

final case class NewEmployee(id: Long, name: String, department: String, mobile: String)