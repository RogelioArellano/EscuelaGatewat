package models

type InstruccionEnvio struct {
	InstruccionID      int          `json:"instruccionID"`
	FechaOperacion     string       `json:"fechaOperacion"`
	ClaveEmisor        int32        `json:"claveEmisor"`
	FolioConsecutivo   int32        `json:"folioConsecutivo"`
	NumAltaEstudiantes int32        `json:"numAltaEstudiantes"`
	InfoAdicional      []Estudiante `json:"infoAdicional"`
}
