package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	// numberOfBlocksDefault is the default number of concurrent blocks the JSON
	// input file will be broken into for processing.
	numberOfBlocksDefault = 16
)

// Area  struct da area, carregando campos extras para serem utilizados durante os calculos ..
type Area struct {
	Codigo             []byte `json:"codigo"`
	Nome               []byte `json:"nome"`
	QTD                int
	AvgSal             float64
	MaxSal             float64
	MinSal             float64
	AreaMinFuncPointer *Employee
	AreaMaxFuncPointer *Employee
	MostAreaQTD        *Area
	LeastAreaQTD       *Area
}

// Employee = funcionario ..
type Employee struct {
	Nome                        []byte  `json:"nome"`
	Sobrenome                   []byte  `json:"sobrenome"`
	Salario                     float64 `json:"salario"`
	Area                        []byte  `json:"area"`
	GlobalMaxSalEmployeePointer *Employee
	GlobalMinSalEmployeePointer *Employee
	AreaMaxSalEmployeePointer   *Employee
	AreaMinSalEmployeePointer   *Employee
}

// JSON união dos 2 para unmarshal ..
type JSON struct {
	EmployeesPointer []Employee `json:"funcionarios"`
	AreasPointer     []Area     `json:"areas"`
}

// MaiorSalGlobal aonde é armazenado o maior valor glogal de salario
type MaiorSalGlobal struct {
	Salario         float64
	EmployeePointer *Employee
}

// MenorSalGlobal aonde é armazenado o menor valor glogal de salario
type MenorSalGlobal struct {
	Salario         float64
	EmployeePointer *Employee
}

// LeastAreaQtd (least_employee) aonde é armazenado a maior(es) area(s) por quantidade de funcionario
type LeastAreaQtd struct {
	QTD          int
	AreasPointer *Area
}

// MostAreaQtd (most_employee)aonde é armazenado a maior(es) area(s) por quantidade de funcionario
type MostAreaQtd struct {
	QTD          int
	AreasPointer *Area
}

type lastNameSal map[string][]*Employee

/*
	calcula o maior salario por sobrenome,
	desconsiderando nomes + sobrenomes
	já existentes durante o calculo.
	Exemplo: se João Vitor, já existir,
	o próximo João Vitor informado é desconsiderado.
*/
func maxSalByLastName(bigSalaryByLastName *lastNameSal, dat JSON, count int) {
	sliceEmployee, found := (*bigSalaryByLastName)[string(dat.EmployeesPointer[count].Sobrenome)]
	if found {
		for _, value := range sliceEmployee {
			if dat.EmployeesPointer[count].Salario == value.Salario {
				if string(value.Nome) == string(dat.EmployeesPointer[count].Nome) {
					return
				}
				(*bigSalaryByLastName)[string(dat.EmployeesPointer[count].Sobrenome)] =
					append((*bigSalaryByLastName)[string(dat.EmployeesPointer[count].Sobrenome)], &dat.EmployeesPointer[count])
				return
			}
			if dat.EmployeesPointer[count].Salario > value.Salario {
				if string(value.Nome) == string(dat.EmployeesPointer[count].Nome) {
					return
				}
				(*bigSalaryByLastName)[string(dat.EmployeesPointer[count].Sobrenome)] = ([]*Employee{&dat.EmployeesPointer[count]})
				return
			}
		}
		return
	}
	(*bigSalaryByLastName)[string(dat.EmployeesPointer[count].Sobrenome)] = []*Employee{&dat.EmployeesPointer[count]}
	return
}

// baseado nesta solução https://github.com/OsProgramadores/op-desafios/blob/master/desafio-05/qrwteyrutiyoup/go/d05.go
//=============================================================================================================

// parseArea parses an area from the input JSON file.
func (d *JSON) parseArea(data []byte) {
	totalQuotes := 0
	var current uint32
	var previous uint32
	a := Area{}
	doublequote := byte('"')
	i := uint32(0)
	var idx int
	for {
		if idx = bytes.IndexByte(data[i:], doublequote); idx == -1 {
			break
		}

		totalQuotes++
		previous = current
		current = i + uint32(idx)
		i = current + 1

		switch totalQuotes {
		// {"codigo":"SM", "nome":"Gerenciamento de Software"}
		case 4:
			a.Codigo = make([]byte, current-previous-1)
			copy(a.Codigo, data[previous+1:current])
		case 8:
			a.Nome = make([]byte, current-previous-1)
			copy(a.Nome, data[previous+1:current])
			d.AreasPointer = append(d.AreasPointer, a)
			return
		}
	}
}

// parseEmployee parses an employee from the input JSON file. If the received
// data is not an employee, it calls parseArea instead.
func (d *JSON) parseEmployee(data []byte, start, end uint32) {
	totalQuotes := 0
	var current uint32
	var previous uint32

	e := Employee{}

	for i := start; i < end; i++ {
		if data[i] != '"' {
			continue
		}
		totalQuotes++
		previous = current
		current = i

		switch totalQuotes {
		// {"id":1,"nome":"Aahron","sobrenome":"Abaine","salario":68379.29,"area":"PI"}
		case 2:
			// Checking if it is an employee.
			if !bytes.Equal([]byte("id"), data[previous+1:current]) {
				d.parseArea(data[start : end+1])
				return
			}
		case 6:
			e.Nome = make([]byte, current-previous-1)
			copy(e.Nome, data[previous+1:current])
		case 10:
			e.Sobrenome = make([]byte, current-previous-1)
			copy(e.Sobrenome, data[previous+1:current])
		case 13:
			j := current - 2
			for ; j > previous; j-- {
				if data[j] >= '0' && data[j] <= '9' {
					break
				}
			}
			salary, err := strconv.ParseFloat(string((data[previous+2 : j+1])), 64)
			if err != nil {
				log.Printf("oops: error converting %q to float: %v\n", data[previous+2:j+1], err)
			}
			e.Salario = salary
		case 16:
			e.Area = make([]byte, current-previous-1)
			copy(e.Area, data[previous+1:current])
			d.EmployeesPointer = append(d.EmployeesPointer, e)
			return
		}
	}
}

// parseJSONBlock parses a block of JSON data from the input file. This method
// will be run concurrently and generate a partial solution from the data it
// processes, then send the result via the `block' channel.
func (d *JSON) parseJSONBlock(data []byte, block chan *JSON) {
	var start uint32

	partialSolution := &JSON{AreasPointer: []Area{}, EmployeesPointer: []Employee{}}

	openbracket := byte('{')
	closedbracket := byte('}')
	i := uint32(0)
	var idx int
	for {
		if idx = bytes.IndexByte(data[i:], openbracket); idx == -1 {
			break
		}
		start = i + uint32(idx)
		i = start

		if idx = bytes.IndexByte(data[i:], closedbracket); idx == -1 {
			break
		}
		i += uint32(idx)
		partialSolution.parseEmployee(data, start, i)
		i++

	}

	block <- partialSolution
}

// parseJSON receives the full JSON data from the input file and calls
// `parseJSONBlocks' to process the smaller blocks. It returns the global
// solution for the problem at hand, once the partial solutions have all been
// accounted for.
func parseJSON(data []byte, blocksToUse uint32) *JSON {
	solution := &JSON{AreasPointer: []Area{}, EmployeesPointer: []Employee{}}
	block := make(chan *JSON)
	wg := sync.WaitGroup{}

	// An average step to form the blocks.
	step := uint32(len(data)) / blocksToUse

	size := uint32(len(data))
	i := step
	start := uint32(1)
	bracket := byte('{')
	var idx int
	for p := uint32(0); p < blocksToUse-1; p++ {
		for i < size {
			if idx = bytes.IndexByte(data[i:], bracket); idx == -1 {
				break
			}

			wg.Add(1)
			i += uint32(idx)
			go solution.parseJSONBlock(data[start:i-1], block)

			start = i
			i += step
			break
		}
	}
	// Last block.
	wg.Add(1)

	go solution.parseJSONBlock(data[start:], block)

	wg.Wait()

	return <-block
}

//==================================================================================================
func main() {
	rawdata, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	numberOfBlocks := uint32(numberOfBlocksDefault)

	dat := parseJSON(rawdata, numberOfBlocks)

	var sizeArea int = len(dat.AreasPointer)

	var mediaGlobalSal float64

	globalMaxSal := MaiorSalGlobal{}
	globalMinSal := MenorSalGlobal{}
	bigSalaryByLastName := lastNameSal{}

	mostArea := MostAreaQtd{}

	var count int
	for count = 0; count < len(dat.EmployeesPointer); count++ {
		// calculo maior salario por sobrenome
		maxSalByLastName(&bigSalaryByLastName, *dat, count)

		// calculo global_max
		if dat.EmployeesPointer[count].Salario > globalMaxSal.Salario {
			globalMaxSal.Salario = dat.EmployeesPointer[count].Salario
			globalMaxSal.EmployeePointer = &dat.EmployeesPointer[count]
		} else if dat.EmployeesPointer[count].Salario == globalMaxSal.Salario {

			temp := globalMaxSal.EmployeePointer
			for temp.GlobalMaxSalEmployeePointer != nil {
				temp = temp.GlobalMaxSalEmployeePointer
			}

			temp.GlobalMaxSalEmployeePointer = &dat.EmployeesPointer[count]
		}

		// calculo glogal_min
		if count == 0 {
			globalMinSal.Salario = dat.EmployeesPointer[count].Salario
			globalMinSal.EmployeePointer = &dat.EmployeesPointer[count]

		} else if dat.EmployeesPointer[count].Salario == globalMinSal.Salario {

			temp := globalMinSal.EmployeePointer
			for temp.GlobalMinSalEmployeePointer != nil {
				temp = temp.GlobalMinSalEmployeePointer
			}
			temp.GlobalMinSalEmployeePointer = &dat.EmployeesPointer[count]

		} else if dat.EmployeesPointer[count].Salario < globalMinSal.Salario {
			globalMinSal.Salario = dat.EmployeesPointer[count].Salario
			globalMinSal.EmployeePointer = &dat.EmployeesPointer[count]

		}

		mediaGlobalSal += dat.EmployeesPointer[count].Salario

		for areaCount := 0; areaCount < sizeArea; areaCount++ {
			if string(dat.EmployeesPointer[count].Area) == string(dat.AreasPointer[areaCount].Codigo) {
				// calculo menor salario
				if dat.AreasPointer[areaCount].QTD == 0 {
					dat.AreasPointer[areaCount].MinSal = dat.EmployeesPointer[count].Salario
					dat.AreasPointer[areaCount].AreaMinFuncPointer = &dat.EmployeesPointer[count]

				} else if dat.EmployeesPointer[count].Salario == dat.AreasPointer[areaCount].MinSal {

					temp := dat.AreasPointer[areaCount].AreaMinFuncPointer
					for temp.AreaMinSalEmployeePointer != nil {
						temp = temp.AreaMinSalEmployeePointer
					}
					temp.AreaMinSalEmployeePointer = &dat.EmployeesPointer[count]

				}

				if dat.EmployeesPointer[count].Salario < dat.AreasPointer[areaCount].MinSal {
					dat.AreasPointer[areaCount].MinSal = dat.EmployeesPointer[count].Salario
					dat.AreasPointer[areaCount].AreaMinFuncPointer = &dat.EmployeesPointer[count]
				}

				dat.AreasPointer[areaCount].QTD++

				// calculo maior salario
				if dat.EmployeesPointer[count].Salario == dat.AreasPointer[areaCount].MaxSal {

					temp := dat.AreasPointer[areaCount].AreaMaxFuncPointer
					for temp.AreaMaxSalEmployeePointer != nil {
						temp = temp.AreaMaxSalEmployeePointer
					}
					temp.AreaMaxSalEmployeePointer = &dat.EmployeesPointer[count]

				}
				if dat.EmployeesPointer[count].Salario > dat.AreasPointer[areaCount].MaxSal {
					dat.AreasPointer[areaCount].MaxSal = dat.EmployeesPointer[count].Salario
					dat.AreasPointer[areaCount].AreaMaxFuncPointer = &dat.EmployeesPointer[count]
				}

				dat.AreasPointer[areaCount].AvgSal += dat.EmployeesPointer[count].Salario

				// calculo das areas most employee
				if dat.AreasPointer[areaCount].QTD == mostArea.QTD {
					temp := mostArea.AreasPointer
					for temp.MostAreaQTD != nil {
						temp = temp.MostAreaQTD
					}
					temp.MostAreaQTD = &dat.AreasPointer[areaCount]
				}
				if dat.AreasPointer[areaCount].QTD > mostArea.QTD {
					mostArea.QTD = dat.AreasPointer[areaCount].QTD
					mostArea.AreasPointer = &dat.AreasPointer[areaCount]
				}

			}
		}
	}

	wg1 := sync.WaitGroup{}
	wg1.Add(8)

	// exibindo global_avg

	os.Stdout.WriteString("global_avg|")
	os.Stdout.WriteString(strconv.FormatFloat(mediaGlobalSal/float64(count), 'f', 2, 64))

	// area avg
	// area_avg|<nome da área>|<salário médio>
	go func() {
		var sb strings.Builder
		for contador := 0; contador < sizeArea; contador++ {
			if dat.AreasPointer[contador].QTD != 0 {
				sb.WriteString("\narea_avg|")
				sb.Write(dat.AreasPointer[contador].Nome)
				sb.WriteString("|")
				sb.WriteString(strconv.FormatFloat(dat.AreasPointer[contador].AvgSal/float64(dat.AreasPointer[contador].QTD), 'f', 2, 64))
			}
		}
		// exibindo o area_avg
		os.Stdout.WriteString(sb.String())

		wg1.Done()
	}()

	// glogal_max
	// global_max|<Nome Completo>|<Salário>
	go func() {
		// construindo a string
		var sb strings.Builder
		sb.WriteString("\nglobal_max|")
		sb.Write(globalMaxSal.EmployeePointer.Nome)
		sb.WriteString(" ")
		sb.Write(globalMaxSal.EmployeePointer.Sobrenome)
		sb.WriteString("|")
		sb.WriteString(strconv.FormatFloat(globalMaxSal.EmployeePointer.Salario, 'f', 2, 64))
		for globalMaxSal.EmployeePointer.GlobalMaxSalEmployeePointer != nil {
			sb.WriteString("\nglobal_max|")
			sb.Write(globalMaxSal.EmployeePointer.GlobalMaxSalEmployeePointer.Nome)
			sb.WriteString(" ")
			sb.Write(globalMaxSal.EmployeePointer.GlobalMaxSalEmployeePointer.Sobrenome)
			sb.WriteString("|")
			sb.WriteString(strconv.FormatFloat(globalMaxSal.EmployeePointer.GlobalMaxSalEmployeePointer.Salario, 'f', 2, 64))
			globalMaxSal.EmployeePointer.GlobalMaxSalEmployeePointer =
				globalMaxSal.EmployeePointer.GlobalMaxSalEmployeePointer.GlobalMaxSalEmployeePointer
		}
		// exibindo
		os.Stdout.WriteString(sb.String())

		wg1.Done()
	}()

	// glogal_min
	// global_min|<nome completo>|<salário>
	go func() {
		// construindo a string
		var sb strings.Builder
		sb.WriteString("\nglobal_min|")
		sb.Write(globalMinSal.EmployeePointer.Nome)
		sb.WriteString(" ")
		sb.Write(globalMinSal.EmployeePointer.Sobrenome)
		sb.WriteString("|")
		sb.WriteString(strconv.FormatFloat(globalMinSal.EmployeePointer.Salario, 'f', 2, 64))
		globalMinSal.EmployeePointer = globalMinSal.EmployeePointer.GlobalMinSalEmployeePointer

		for globalMinSal.EmployeePointer != nil {
			sb.WriteString("\nglobal_min|")
			sb.Write(globalMinSal.EmployeePointer.Nome)
			sb.WriteString(" ")
			sb.Write(globalMinSal.EmployeePointer.Sobrenome)
			sb.WriteString("|")
			sb.WriteString(strconv.FormatFloat(globalMinSal.EmployeePointer.Salario, 'f', 2, 64))
			globalMinSal.EmployeePointer = globalMinSal.EmployeePointer.GlobalMinSalEmployeePointer
		}
		// exibindo
		os.Stdout.WriteString(sb.String())

		wg1.Done()
	}()

	// least_employe
	// least_employees|<nome da área>|<número de funcionários>
	go func() {
		leastArea := LeastAreaQtd{}
		leastArea.QTD = mostArea.QTD
		// calculando
		for contador := 1; contador < sizeArea; contador++ {
			if dat.AreasPointer[contador].QTD != 0 {
				if dat.AreasPointer[contador].QTD == leastArea.QTD {
					temp := leastArea.AreasPointer
					for temp.LeastAreaQTD != nil {
						temp = temp.LeastAreaQTD
					}
					temp.LeastAreaQTD = &dat.AreasPointer[contador]
				}
				if dat.AreasPointer[contador].QTD < leastArea.QTD {
					leastArea.QTD = dat.AreasPointer[contador].QTD
					leastArea.AreasPointer = &dat.AreasPointer[contador]
				}
			}

		}
		// construindo a string
		var sb strings.Builder
		sb.Write([]byte("\nleast_employees|"))
		sb.Write(leastArea.AreasPointer.Nome)
		sb.Write([]byte("|"))
		sb.WriteString(strconv.Itoa(leastArea.QTD))
		leastArea.AreasPointer = leastArea.AreasPointer.LeastAreaQTD
		for leastArea.AreasPointer != nil {
			sb.WriteString("\nleast_employees|")
			sb.Write(dat.AreasPointer[0].Nome)
			sb.WriteString("|")
			sb.WriteString(strconv.Itoa(dat.AreasPointer[0].QTD))
			leastArea.AreasPointer = leastArea.AreasPointer.LeastAreaQTD
		}
		// exibindo
		os.Stdout.WriteString(sb.String())

		wg1.Done()
	}()

	// most_employees
	// most_employees|<nome da área>|<número de funcionários>
	go func() {
		// construindo a string
		var sb strings.Builder
		sb.WriteString("\nmost_employees|")
		sb.Write(mostArea.AreasPointer.Nome)
		sb.WriteString("|")
		sb.WriteString(strconv.Itoa(mostArea.QTD))
		mostArea.AreasPointer = mostArea.AreasPointer.MostAreaQTD
		for mostArea.AreasPointer != nil {
			sb.WriteString("\nmost_employees|")
			sb.Write(mostArea.AreasPointer.Nome)
			sb.WriteString("|")
			sb.WriteString(strconv.Itoa(mostArea.QTD))
			mostArea.AreasPointer = mostArea.AreasPointer.MostAreaQTD
		}
		// exibindo
		os.Stdout.WriteString(sb.String())

		wg1.Done()
	}()

	// area_max
	// area_max|<nome da área>|<nome completo>|<salário máximo>
	go func() {
		// construindo 1 string para, com o valor de todas as areas
		var sb strings.Builder
		for contador := 0; contador < sizeArea; contador++ {
			if dat.AreasPointer[contador].QTD != 0 {
				sb.WriteString("\narea_max|")
				sb.Write(dat.AreasPointer[contador].Nome)
				sb.WriteString("|")
				sb.Write(dat.AreasPointer[contador].AreaMaxFuncPointer.Nome)
				sb.WriteString(" ")
				sb.Write(dat.AreasPointer[contador].AreaMaxFuncPointer.Sobrenome)
				sb.WriteString("|")
				sb.WriteString(strconv.FormatFloat(dat.AreasPointer[contador].AreaMaxFuncPointer.Salario, 'f', 2, 64))
				dat.AreasPointer[contador].AreaMaxFuncPointer = dat.AreasPointer[contador].AreaMaxFuncPointer.AreaMaxSalEmployeePointer
				for dat.AreasPointer[contador].AreaMaxFuncPointer != nil {
					sb.WriteString("\narea_max|")
					sb.Write(dat.AreasPointer[contador].Nome)
					sb.WriteString("|")
					sb.Write(dat.AreasPointer[contador].AreaMaxFuncPointer.Nome)
					sb.WriteString(" ")
					sb.Write(dat.AreasPointer[contador].AreaMaxFuncPointer.Sobrenome)
					sb.WriteString("|")
					sb.WriteString(strconv.FormatFloat(dat.AreasPointer[contador].AreaMaxFuncPointer.Salario, 'f', 2, 64))
					dat.AreasPointer[contador].AreaMaxFuncPointer = dat.AreasPointer[contador].AreaMaxFuncPointer.AreaMaxSalEmployeePointer
				}
			}
		}

		// exibindo
		os.Stdout.WriteString(sb.String())

		wg1.Done()
	}()

	//  area_min
	//  area_min|<nome da área>|<nome completo>|<salário>
	go func() {
		// construindo a string
		var sb strings.Builder
		// construindo 1 string para, com o valor de todas as areas
		for contador := 0; contador < sizeArea; contador++ {
			if dat.AreasPointer[contador].QTD != 0 {
				sb.WriteString("\narea_min|")
				sb.Write(dat.AreasPointer[contador].Nome)
				sb.WriteString("|")
				sb.Write(dat.AreasPointer[contador].AreaMinFuncPointer.Nome)
				sb.WriteString(" ")
				sb.Write(dat.AreasPointer[contador].AreaMinFuncPointer.Sobrenome)
				sb.WriteString("|")
				sb.WriteString(strconv.FormatFloat(dat.AreasPointer[contador].AreaMinFuncPointer.Salario, 'f', 2, 64))
				dat.AreasPointer[contador].AreaMinFuncPointer = dat.AreasPointer[contador].AreaMinFuncPointer.AreaMinSalEmployeePointer
				for dat.AreasPointer[contador].AreaMinFuncPointer != nil {
					sb.WriteString("\narea_min|")
					sb.Write(dat.AreasPointer[contador].Nome)
					sb.WriteString("|")
					sb.Write(dat.AreasPointer[contador].AreaMinFuncPointer.Nome)
					sb.WriteString(" ")
					sb.Write(dat.AreasPointer[contador].AreaMinFuncPointer.Sobrenome)
					sb.WriteString("|")
					sb.WriteString(strconv.FormatFloat(dat.AreasPointer[contador].AreaMinFuncPointer.Salario, 'f', 2, 64))
					dat.AreasPointer[contador].AreaMinFuncPointer = dat.AreasPointer[contador].AreaMinFuncPointer.AreaMinSalEmployeePointer
				}
			}
		}
		// exibindo
		os.Stdout.WriteString(sb.String())

		wg1.Done()
	}()

	// last_name_max
	// last_name_max|<sobrenome do funcionário>|<nome completo>|<salário>
	go func() {
		// construindo a string
		var sb strings.Builder
		for sobreNome, arrayFuncs := range bigSalaryByLastName {
			for idx := 0; idx < len(arrayFuncs); idx++ {
				sb.WriteString("\nlast_name_max|")
				sb.WriteString(sobreNome)
				sb.WriteString("|")
				sb.Write(arrayFuncs[idx].Nome)
				sb.WriteString(" ")
				sb.WriteString(sobreNome)
				sb.WriteString("|")
				sb.WriteString(strconv.FormatFloat(arrayFuncs[idx].Salario, 'f', 2, 64))
			}
		}
		// exibindo
		os.Stdout.WriteString(sb.String())
		wg1.Done()
	}()

	wg1.Wait()

	os.Stdout.WriteString("\n")

}
