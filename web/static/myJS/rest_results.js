$( document ).ready(function() {
    var start = 0;
    var size = 100;

    var iid = setInterval(function(){
        $.post("get_rest", {"start": start, "size": size }, function(data){
        console.log(data);
        if(data){
            $("#rest-placeholder").append(data);
        } else {
            clearInterval(iid);
        }
        });
        start += size;
    }, 1000);

});