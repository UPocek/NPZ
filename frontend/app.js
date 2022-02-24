$('.toggle').on('click', function () {
    $('.container').stop().addClass('active');
});

$('.close').on('click', function () {
    $('.container').stop().removeClass('active');
});

// Login Form

let forma = document.getElementById('form1');
forma.addEventListener('submit', function (e) {
    e.preventDefault();
    let request = new XMLHttpRequest();

    request.onreadystatechange = function () {
        if (this.readyState == 4) {
            if (this.status == 200) {
                console.log("Successful Login");
                window.location.replace('profile.html?username=' + username.value);
            }
        }
    }

    let username = document.getElementsByClassName("user1")[0];
    let password = document.getElementsByClassName("pass1")[0];

    if (username.value.trim().length > 0 && password.value.trim().length > 0) {
        request.open('GET', 'https://npz-nasp.herokuapp.com/login/' + username.value + "," + password.value);
        request.send();
    } else {
        alert("Username ili password ne smeju ostati prazni. Prvo ih popunite pa pokušajte ponovo.")
    }
});

// Registartion Form

let forma2 = document.getElementById('form2');
forma2.addEventListener('submit', function (e) {
    e.preventDefault();
    let request = new XMLHttpRequest();

    request.onreadystatechange = function () {
        if (this.readyState == 4) {
            if (this.status == 200) {
                console.log("Successful Registration");
                alert("Successful Registration");
            }
        }
    }

    let username2 = document.getElementsByClassName("user2")[0];
    let password2 = document.getElementsByClassName("pass2")[0];
    let password3 = document.getElementsByClassName("pass3")[0];

    if (username2.value.trim().length > 0 && password2.value.trim().length > 0 && password3.value.trim().length > 0 && (password2.value.trim() == password3.value.trim())) {
        request.open('POST', 'https://npz-nasp.herokuapp.com/login/' + username2.value + "," + password2.value);
        request.send();
    } else {
        alert("Username ili password ne smeju ostati prazni. Prvo ih popunite pa pokušajte ponovo.")
    }
});