package main

import (
	"Projekat/App"
	"fmt"
	"strings"
)

func main() {
	app := App.CreateApp()

	for true {
		fmt.Print("Izaberite jednu od opcija:\n 1) Put \n 2) Get \n 3) Delete \n 4) Test \n 5) Exit \n >> ")
		var option string
		fmt.Scanln(&option)
		if strings.Replace(option, ")", "", 1) == "1" {
			fmt.Print("Unesite ključ\n >> ")
			var key string
			fmt.Scanln(&key)
			fmt.Print("Unesite vrednost\n >> ")
			var value string
			fmt.Scanln(&value)
			ok := app.Put(key, []byte(value))
			if ok {
				fmt.Println("\nDodavanje elementa je uspešno odradjeno!\n")
			} else {
				fmt.Println("\nDošlo je do greške prilikom dodavanja elementa!\n")
			}
		} else if strings.Replace(option, ")", "", 1) == "2" {
			fmt.Print("Unesite ključ\n >> ")
			var key string
			fmt.Scanln(&key)
			ok, value := app.Get(key)
			if ok {
				fmt.Println("\nVrednost ključa " + key + " je " + string(value) + "\n")
			} else {
				fmt.Println(value)
			}
		} else if strings.Replace(option, ")", "", 1) == "3" {
			fmt.Print("Unesite ključ\n >> ")
			var key string
			fmt.Scanln(&key)
			fmt.Print("Unesite vrednost\n >> ")
			var value string
			fmt.Scanln(&value)
			ok := app.Delete(key, []byte(value))
			if ok {
				fmt.Println("\nBrisanje elementa je uspešno odradjeno!\n")
			} else {
				fmt.Println("\nDošlo je do greške prilikom brisanja elementa!\n")
			}
		} else if strings.Replace(option, ")", "", 1) == "4" {
			ok := app.Test()
			if !ok {
				fmt.Println("\nGreška prilikom testiranja\n")
			}
		} else if strings.Replace(option, ")", "", 1) == "5" {
			break
		} else {
			fmt.Println("\nUneta opcija ne postoji pokušajte ponovo.\n")
		}
	}

	app.StopApp()
}
