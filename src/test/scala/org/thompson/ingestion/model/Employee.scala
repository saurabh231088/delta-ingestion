package org.thompson.ingestion.model

final case class Employee(id: Int, name: String, department: String)

final case class NewEmployee(id: Int, name: String, department: String, mobile: String)