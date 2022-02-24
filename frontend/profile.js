function getParamValue(name) {
    var location = decodeURI(window.location.toString());
    var index = location.indexOf("?") + 1;
    var subs = location.substring(index, location.length);
    var splitted = subs.split("&");

    for (var i = 0; i < splitted.length; i++) {
        var s = splitted[i].split("=");
        var pName = s[0];
        var pValue = s[1];
        if (pName == name)
            return pValue;
    }
}

let username = getParamValue('username')

let uspan = document.getElementById('sayhello')
uspan.innerText = username

// Form

inputspan = document.getElementById('result');

let forma = document.getElementById('submit-form');
forma.addEventListener('submit', function (e) {
    e.preventDefault();
    let request = new XMLHttpRequest();

    request.onreadystatechange = function () {
        if (this.readyState == 4) {
            if (this.status == 200) {
                if (what.value == "GET") {
                    result = this.responseText;
                    inputspan.innerText = result;
                } else if (what.value == "POST") {
                    inputspan.innerText = "Podatak je uspešno upisan";
                } else if (what.value == "PUT") {
                    inputspan.innerText = "Podatak je uspešno ažuriran";
                } else if (what.value == "DELETE") {
                    inputspan.innerText = "Podatak je uspešno obrisan";
                }
                else {
                    alert("Nevalidan metod!")
                }
            } else {
                if (what.value == "GET") {
                    inputspan.innerText = "Traženi podatak ne postoji";
                } else if (what.value == "POST") {
                    inputspan.innerText = "Podatak nije uspešno upisan";
                } else if (what.value == "PUT") {
                    inputspan.innerText = "Podatak nije uspešno ažuriran";
                } else if (what.value == "DELETE") {
                    inputspan.innerText = "Podatak nije uspešno obrisan";
                }
                else {
                    alert("Nevalidan metod!")
                }
            }
        }
    }

    let key = document.getElementsByClassName("user1")[0];
    let value = document.getElementsByClassName("pass1")[0];
    let what = document.getElementById("what");

    if (key.value.trim().length > 0) {

        if (what.value == "GET") {
            request.open('GET', 'https://npz-nasp.herokuapp.com/data/' + key.value);
        } else if (what.value == "POST") {
            request.open('POST', 'https://npz-nasp.herokuapp.com/data/' + key.value + ',' + value.value);
        } else if (what.value == "PUT") {
            request.open('POST', 'https://npz-nasp.herokuapp.com/data/' + key.value + ',' + value.value);
        } else if (what.value == "DELETE") {
            request.open('DELETE', 'https://npz-nasp.herokuapp.com/data/' + key.value + ',' + value.value);
        }
        else {
            alert("Nevalidan metod!")
        }
        request.send();
    } else {
        alert("Key je ostao prazan. Prvo ga popunite pa pokušajte ponovo.")
    }
});