[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "wind_power_data_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "production_id", DestinationColumnName = "production_id"], [SourceColumnName = "date", DestinationColumnName = "date"], [SourceColumnName = "time", DestinationColumnName = "time"], [SourceColumnName = "turbine_name", DestinationColumnName = "turbine_name"], [SourceColumnName = "capacity", DestinationColumnName = "capacity"], [SourceColumnName = "location_name", DestinationColumnName = "location_name"], [SourceColumnName = "latitude", DestinationColumnName = "latitude"], [SourceColumnName = "longitude", DestinationColumnName = "longitude"], [SourceColumnName = "region", DestinationColumnName = "region"], [SourceColumnName = "status", DestinationColumnName = "status"], [SourceColumnName = "responsible_department", DestinationColumnName = "responsible_department"], [SourceColumnName = "wind_speed", DestinationColumnName = "wind_speed"], [SourceColumnName = "wind_direction", DestinationColumnName = "wind_direction"], [SourceColumnName = "energy_produced", DestinationColumnName = "energy_produced"], [SourceColumnName = "day", DestinationColumnName = "day"], [SourceColumnName = "month", DestinationColumnName = "month"], [SourceColumnName = "quarter", DestinationColumnName = "quarter"], [SourceColumnName = "year", DestinationColumnName = "year"], [SourceColumnName = "hour_of_day", DestinationColumnName = "hour_of_day"], [SourceColumnName = "minute_of_hour", DestinationColumnName = "minute_of_hour"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared wind_power_data = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "701df2d7-7e7b-408d-926d-8a0235da76a7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "3d3022ed-2651-4919-b627-8d42ddf1f2cf"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "wind_power_data", ItemKind = "Table"]}[Data],
  #"Rounded off" = Table.TransformColumns(#"Navigation 2", {{"wind_speed", each Number.Round(_, 2), type number}}),
  #"Inserted day" = Table.AddColumn(#"Rounded off", "Day", each Date.Day([date]), type nullable number),
  #"Inserted month" = Table.AddColumn(#"Inserted day", "Month", each Date.Month([date]), type nullable number),
  #"Inserted year" = Table.AddColumn(#"Inserted month", "Year", each Date.Year([date]), type nullable number),
  #"Inserted quarter" = Table.AddColumn(#"Inserted year", "Quarter", each Date.QuarterOfYear([date]), type nullable number),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted quarter", {{"Day", Int64.Type}, {"Month", Int64.Type}, {"Year", Int64.Type}, {"Quarter", Int64.Type}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"Day", "day"}, {"Month", "month"}, {"Year", "year"}, {"Quarter", "quarter"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "-", ":", Replacer.ReplaceText, {"time"}),
  #"Duplicated column" = Table.DuplicateColumn(#"Replaced value", "time", "time - Copy"),
  #"Split column by delimiter" = Table.SplitColumn(#"Duplicated column", "time - Copy", Splitter.SplitTextByDelimiter(":"), {"time - Copy.1", "time - Copy.2"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Split column by delimiter", {{"time - Copy.1", Int64.Type}, {"time - Copy.2", Int64.Type}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 1", {{"time - Copy.1", "hour_of_day"}, {"time - Copy.2", "minute_of_hour"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns 1", "time_period", each if [hour_of_day] >= 5 and [hour_of_day] < 12 then "Morning"
else if [hour_of_day] >= 12 and [hour_of_day] < 17 then "Afternoon"
else if [hour_of_day] >= 17 and [hour_of_day] < 21 then "Evening"
else "Night")
in
  #"Added custom";
shared wind_power_data_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "701df2d7-7e7b-408d-926d-8a0235da76a7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8e266899-19e4-4df0-942b-a275daa51e4a"]}[Data],
  TableNavigation = Navigation_2{[Id = "wind_power", ItemKind = "Table"]}[Data]
in
  TableNavigation;
